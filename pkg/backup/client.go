// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pingcap/br/pkg/metautil"

	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/br/pkg/conn"
	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/redact"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

// ClientMgr manages connections needed by backup.
type ClientMgr interface {
	GetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error)
	ResetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error)
	GetPDClient() pd.Client
	GetLockResolver() *txnlock.LockResolver
	Close()
}

// Checksum is the checksum of some backup files calculated by CollectChecksums.
type Checksum struct {
	Crc64Xor   uint64
	TotalKvs   uint64
	TotalBytes uint64
}

// ProgressUnit represents the unit of progress.
type ProgressUnit string

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	backupFineGrainedMaxBackoff = 80000
	backupRetryTimes            = 5
	// RangeUnit represents the progress updated counter when a range finished.
	RangeUnit ProgressUnit = "range"
	// RegionUnit represents the progress updated counter when a region finished.
	RegionUnit ProgressUnit = "region"
)

// Client is a client instructs TiKV how to do a backup.
type Client struct {
	mgr       ClientMgr
	clusterID uint64

	storage storage.ExternalStorage
	backend *backuppb.StorageBackend

	gcTTL int64
}

// NewBackupClient returns a new backup client.
func NewBackupClient(ctx context.Context, mgr ClientMgr) (*Client, error) {
	log.Info("new backup client")
	pdClient := mgr.GetPDClient()
	clusterID := pdClient.GetClusterID(ctx)
	return &Client{
		clusterID: clusterID,
		mgr:       mgr,
	}, nil
}

// GetTS returns the latest timestamp.
func (bc *Client) GetTS(ctx context.Context, duration time.Duration, ts uint64) (uint64, error) {
	var (
		backupTS uint64
		err      error
	)
	if ts > 0 {
		backupTS = ts
	} else {
		p, l, err := bc.mgr.GetPDClient().GetTS(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		backupTS = oracle.ComposeTS(p, l)

		switch {
		case duration < 0:
			return 0, errors.Annotate(berrors.ErrInvalidArgument, "negative timeago is not allowed")
		case duration > 0:
			log.Info("backup time ago", zap.Duration("timeago", duration))

			backupTime := oracle.GetTimeFromTS(backupTS)
			backupAgo := backupTime.Add(-duration)
			if backupTS < oracle.ComposeTS(oracle.GetPhysical(backupAgo), l) {
				return 0, errors.Annotate(berrors.ErrInvalidArgument, "backup ts overflow please choose a smaller timeago")
			}
			backupTS = oracle.ComposeTS(oracle.GetPhysical(backupAgo), l)
		}
	}

	// check backup time do not exceed GCSafePoint
	err = utils.CheckGCSafePoint(ctx, bc.mgr.GetPDClient(), backupTS)
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("backup encode timestamp", zap.Uint64("BackupTS", backupTS))
	return backupTS, nil
}

// SetLockFile set write lock file.
func (bc *Client) SetLockFile(ctx context.Context) error {
	return bc.storage.WriteFile(ctx, metautil.LockFile,
		[]byte("DO NOT DELETE\n"+
			"This file exists to remind other backup jobs won't use this path"))
}

// SetGCTTL set gcTTL for client.
func (bc *Client) SetGCTTL(ttl int64) {
	if ttl <= 0 {
		ttl = utils.DefaultBRGCSafePointTTL
	}
	bc.gcTTL = ttl
}

// GetGCTTL get gcTTL for this backup.
func (bc *Client) GetGCTTL() int64 {
	return bc.gcTTL
}

// GetStorage gets storage for this backup.
func (bc *Client) GetStorage() storage.ExternalStorage {
	return bc.storage
}

// SetStorage set ExternalStorage for client.
func (bc *Client) SetStorage(ctx context.Context, backend *backuppb.StorageBackend, opts *storage.ExternalStorageOptions) error {
	var err error
	bc.storage, err = storage.New(ctx, backend, opts)
	if err != nil {
		return errors.Trace(err)
	}
	// backupmeta already exists
	exist, err := bc.storage.FileExists(ctx, metautil.MetaFile)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", metautil.MetaFile)
	}
	if exist {
		return errors.Annotatef(berrors.ErrInvalidArgument, "backup meta file exists in %v, "+
			"there may be some backup files in the path already, "+
			"please specify a correct backup directory!", bc.storage.URI()+"/"+metautil.MetaFile)
	}
	exist, err = bc.storage.FileExists(ctx, metautil.LockFile)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", metautil.LockFile)
	}
	if exist {
		return errors.Annotatef(berrors.ErrInvalidArgument, "backup lock file exists in %v, "+
			"there may be some backup files in the path already, "+
			"please specify a correct backup directory!", bc.storage.URI()+"/"+metautil.LockFile)
	}
	bc.backend = backend
	return nil
}

// GetClusterID returns the cluster ID of the tidb cluster to backup.
func (bc *Client) GetClusterID() uint64 {
	return bc.clusterID
}

// BuildTableRanges returns the key ranges encompassing the entire table,
// and its partitions if exists.
func BuildTableRanges(tbl *model.TableInfo) ([]kv.KeyRange, error) {
	pis := tbl.GetPartitionInfo()
	if pis == nil {
		// Short path, no partition.
		return appendRanges(tbl, tbl.ID)
	}

	ranges := make([]kv.KeyRange, 0, len(pis.Definitions)*(len(tbl.Indices)+1)+1)
	for _, def := range pis.Definitions {
		rgs, err := appendRanges(tbl, def.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ranges = append(ranges, rgs...)
	}
	return ranges, nil
}

func appendRanges(tbl *model.TableInfo, tblID int64) ([]kv.KeyRange, error) {
	var ranges []*ranger.Range
	if tbl.IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}

	kvRanges, err := distsql.TableHandleRangesToKVRanges(nil, []int64{tblID}, tbl.IsCommonHandle, ranges, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, index := range tbl.Indices {
		if index.State != model.StatePublic {
			continue
		}
		ranges = ranger.FullRange()
		idxRanges, err := distsql.IndexRangesToKVRanges(nil, tblID, index.ID, ranges, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		kvRanges = append(kvRanges, idxRanges...)
	}
	return kvRanges, nil
}

// BuildBackupRangeAndSchema gets KV range and schema of tables.
// KV ranges are separated by Table IDs.
// Also, KV ranges are separated by Index IDs in the same table.
func BuildBackupRangeAndSchema(
	storage kv.Storage,
	tableFilter filter.Filter,
	backupTS uint64,
) ([]rtree.Range, *Schemas, error) {
	snapshot := storage.GetSnapshot(kv.NewVersion(backupTS))
	m := meta.NewSnapshotMeta(snapshot)

	ranges := make([]rtree.Range, 0)
	backupSchemas := newBackupSchemas()
	dbs, err := m.ListDatabases()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, dbInfo := range dbs {
		// skip system databases
		if !tableFilter.MatchSchema(dbInfo.Name.O) || util.IsMemDB(dbInfo.Name.L) {
			continue
		}

		idAlloc := autoid.NewAllocator(storage, dbInfo.ID, false, autoid.RowIDAllocType)
		seqAlloc := autoid.NewAllocator(storage, dbInfo.ID, false, autoid.SequenceType)
		randAlloc := autoid.NewAllocator(storage, dbInfo.ID, false, autoid.AutoRandomType)

		tables, err := m.ListTables(dbInfo.ID)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		if len(tables) == 0 {
			log.Warn("It's not necessary for backing up empty database",
				zap.Stringer("db", dbInfo.Name))
			continue
		}

		for _, tableInfo := range tables {
			if !tableFilter.MatchTable(dbInfo.Name.O, tableInfo.Name.O) {
				// Skip tables other than the given table.
				continue
			}

			logger := log.With(
				zap.String("db", dbInfo.Name.O),
				zap.String("table", tableInfo.Name.O),
			)

			var globalAutoID int64
			switch {
			case tableInfo.IsSequence():
				globalAutoID, err = seqAlloc.NextGlobalAutoID(tableInfo.ID)
			case tableInfo.IsView() || !utils.NeedAutoID(tableInfo):
				// no auto ID for views or table without either rowID nor auto_increment ID.
			default:
				globalAutoID, err = idAlloc.NextGlobalAutoID(tableInfo.ID)
			}
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			tableInfo.AutoIncID = globalAutoID

			if tableInfo.PKIsHandle && tableInfo.ContainsAutoRandomBits() {
				// this table has auto_random id, we need backup and rebase in restoration
				var globalAutoRandID int64
				globalAutoRandID, err = randAlloc.NextGlobalAutoID(tableInfo.ID)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				tableInfo.AutoRandID = globalAutoRandID
				logger.Debug("change table AutoRandID",
					zap.Int64("AutoRandID", globalAutoRandID))
			}
			logger.Debug("change table AutoIncID",
				zap.Int64("AutoIncID", globalAutoID))

			// remove all non-public indices
			n := 0
			for _, index := range tableInfo.Indices {
				if index.State == model.StatePublic {
					tableInfo.Indices[n] = index
					n++
				}
			}
			tableInfo.Indices = tableInfo.Indices[:n]

			backupSchemas.addSchema(dbInfo, tableInfo)

			tableRanges, err := BuildTableRanges(tableInfo)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			for _, r := range tableRanges {
				ranges = append(ranges, rtree.Range{
					StartKey: r.StartKey,
					EndKey:   r.EndKey,
				})
			}
		}
	}

	if backupSchemas.Len() == 0 {
		log.Info("nothing to backup")
		return nil, nil, nil
	}
	return ranges, backupSchemas, nil
}

// WriteBackupDDLJobs sends the ddl jobs are done in (lastBackupTS, backupTS] to metaWriter.
func WriteBackupDDLJobs(metaWriter *metautil.MetaWriter, store kv.Storage, lastBackupTS, backupTS uint64) error {
	snapshot := store.GetSnapshot(kv.NewVersion(backupTS))
	snapMeta := meta.NewSnapshotMeta(snapshot)
	lastSnapshot := store.GetSnapshot(kv.NewVersion(lastBackupTS))
	lastSnapMeta := meta.NewSnapshotMeta(lastSnapshot)
	lastSchemaVersion, err := lastSnapMeta.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}
	allJobs := make([]*model.Job, 0)
	defaultJobs, err := snapMeta.GetAllDDLJobsInQueue(meta.DefaultJobListKey)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("get default jobs", zap.Int("jobs", len(defaultJobs)))
	allJobs = append(allJobs, defaultJobs...)
	addIndexJobs, err := snapMeta.GetAllDDLJobsInQueue(meta.AddIndexJobListKey)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("get add index jobs", zap.Int("jobs", len(addIndexJobs)))
	allJobs = append(allJobs, addIndexJobs...)
	historyJobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("get history jobs", zap.Int("jobs", len(historyJobs)))
	allJobs = append(allJobs, historyJobs...)

	count := 0
	for _, job := range allJobs {
		if (job.State == model.JobStateDone || job.State == model.JobStateSynced) &&
			(job.BinlogInfo != nil && job.BinlogInfo.SchemaVersion > lastSchemaVersion) {
			jobBytes, err := json.Marshal(job)
			if err != nil {
				return errors.Trace(err)
			}
			metaWriter.Send(jobBytes, metautil.AppendDDL)
			count++
		}
	}
	log.Debug("get completed jobs", zap.Int("jobs", count))
	return nil
}

// BackupRanges make a backup of the given key ranges.
func (bc *Client) BackupRanges(
	ctx context.Context,
	ranges []rtree.Range,
	req backuppb.BackupRequest,
	concurrency uint,
	metaWriter *metautil.MetaWriter,
	progressCallBack func(ProgressUnit),
) error {
	init := time.Now()
	defer log.Info("Backup Ranges", zap.Duration("take", time.Since(init)))

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.BackupRanges", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// we collect all files in a single goroutine to avoid thread safety issues.
	workerPool := utils.NewWorkerPool(concurrency, "Ranges")
	eg, ectx := errgroup.WithContext(ctx)
	for _, r := range ranges {
		sk, ek := r.StartKey, r.EndKey
		workerPool.ApplyOnErrorGroup(eg, func() error {
			err := bc.BackupRange(ectx, sk, ek, req, metaWriter, progressCallBack)
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	return eg.Wait()
}

// BackupRange make a backup of the given key range.
// Returns an array of files backed up.
func (bc *Client) BackupRange(
	ctx context.Context,
	startKey, endKey []byte,
	req backuppb.BackupRequest,
	metaWriter *metautil.MetaWriter,
	progressCallBack func(ProgressUnit),
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("backup range finished", zap.Duration("take", elapsed))
		key := "range start:" + hex.EncodeToString(startKey) + " end:" + hex.EncodeToString(endKey)
		if err != nil {
			summary.CollectFailureUnit(key, err)
		}
	}()
	log.Info("backup started",
		logutil.Key("startKey", startKey),
		logutil.Key("endKey", endKey),
		zap.Uint64("rateLimit", req.RateLimit),
		zap.Uint32("concurrency", req.Concurrency))

	var allStores []*metapb.Store
	allStores, err = conn.GetAllTiKVStores(ctx, bc.mgr.GetPDClient(), conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}

	req.StartKey = startKey
	req.EndKey = endKey
	req.StorageBackend = bc.backend

	push := newPushDown(bc.mgr, len(allStores))

	var results rtree.RangeTree
	results, err = push.pushBackup(ctx, req, allStores, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("finish backup push down", zap.Int("Ok", results.Len()))

	// Find and backup remaining ranges.
	// TODO: test fine grained backup.
	err = bc.fineGrainedBackup(
		ctx, startKey, endKey, req.StartVersion, req.EndVersion, req.CompressionType, req.CompressionLevel,
		req.RateLimit, req.Concurrency, results, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}

	// update progress of range unit
	progressCallBack(RangeUnit)

	if req.IsRawKv {
		log.Info("backup raw ranges",
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey),
			zap.String("cf", req.Cf))
	} else {
		log.Info("backup time range",
			zap.Reflect("StartVersion", req.StartVersion),
			zap.Reflect("EndVersion", req.EndVersion))
	}

	var ascendErr error
	results.Ascend(func(i btree.Item) bool {
		r := i.(*rtree.Range)
		for _, f := range r.Files {
			summary.CollectSuccessUnit(summary.TotalKV, 1, f.TotalKvs)
			summary.CollectSuccessUnit(summary.TotalBytes, 1, f.TotalBytes)
		}
		// we need keep the files in order after we support multi_ingest sst.
		// default_sst and write_sst need to be together.
		if err := metaWriter.Send(r.Files, metautil.AppendDataFile); err != nil {
			ascendErr = err
			return false
		}
		return true
	})
	if ascendErr != nil {
		return errors.Trace(ascendErr)
	}

	// Check if there are duplicated files.
	checkDupFiles(&results)

	return nil
}

func (bc *Client) findRegionLeader(ctx context.Context, key []byte) (*metapb.Peer, error) {
	// Keys are saved in encoded format in TiKV, so the key must be encoded
	// in order to find the correct region.
	key = codec.EncodeBytes([]byte{}, key)
	for i := 0; i < 5; i++ {
		// better backoff.
		region, err := bc.mgr.GetPDClient().GetRegion(ctx, key)
		if err != nil || region == nil {
			log.Error("find leader failed", zap.Error(err), zap.Reflect("region", region))
			time.Sleep(time.Millisecond * time.Duration(100*i))
			continue
		}
		if region.Leader != nil {
			log.Info("find leader",
				zap.Reflect("Leader", region.Leader), logutil.Key("key", key))
			return region.Leader, nil
		}
		log.Warn("no region found", logutil.Key("key", key))
		time.Sleep(time.Millisecond * time.Duration(100*i))
		continue
	}
	log.Error("can not find leader", logutil.Key("key", key))
	return nil, errors.Annotatef(berrors.ErrBackupNoLeader, "can not find leader")
}

func (bc *Client) fineGrainedBackup(
	ctx context.Context,
	startKey, endKey []byte,
	lastBackupTS uint64,
	backupTS uint64,
	compressType backuppb.CompressionType,
	compressLevel int32,
	rateLimit uint64,
	concurrency uint32,
	rangeTree rtree.RangeTree,
	progressCallBack func(ProgressUnit),
) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.fineGrainedBackup", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	failpoint.Inject("hint-fine-grained-backup", func(v failpoint.Value) {
		log.Info("failpoint hint-fine-grained-backup injected, "+
			"process will sleep for 3s and notify the shell.", zap.String("file", v.(string)))
		if sigFile, ok := v.(string); ok {
			file, err := os.Create(sigFile)
			if err != nil {
				log.Warn("failed to create file for notifying, skipping notify", zap.Error(err))
			}
			if file != nil {
				file.Close()
			}
			time.Sleep(3 * time.Second)
		}
	})

	bo := tikv.NewBackoffer(ctx, backupFineGrainedMaxBackoff)
	for {
		// Step1, check whether there is any incomplete range
		incomplete := rangeTree.GetIncompleteRange(startKey, endKey)
		if len(incomplete) == 0 {
			return nil
		}
		log.Info("start fine grained backup", zap.Int("incomplete", len(incomplete)))
		// Step2, retry backup on incomplete range
		respCh := make(chan *backuppb.BackupResponse, 4)
		errCh := make(chan error, 4)
		retry := make(chan rtree.Range, 4)

		max := &struct {
			ms int
			mu sync.Mutex
		}{}
		wg := new(sync.WaitGroup)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			fork, _ := bo.Fork()
			go func(boFork *tikv.Backoffer) {
				defer wg.Done()
				for rg := range retry {
					backoffMs, err :=
						bc.handleFineGrained(ctx, boFork, rg, lastBackupTS, backupTS,
							compressType, compressLevel, rateLimit, concurrency, respCh)
					if err != nil {
						errCh <- err
						return
					}
					if backoffMs != 0 {
						max.mu.Lock()
						if max.ms < backoffMs {
							max.ms = backoffMs
						}
						max.mu.Unlock()
					}
				}
			}(fork)
		}

		// Dispatch rangs and wait
		go func() {
			for _, rg := range incomplete {
				retry <- rg
			}
			close(retry)
			wg.Wait()
			close(respCh)
		}()

	selectLoop:
		for {
			select {
			case err := <-errCh:
				// TODO: should we handle err here?
				return errors.Trace(err)
			case resp, ok := <-respCh:
				if !ok {
					// Finished.
					break selectLoop
				}
				if resp.Error != nil {
					log.Panic("unexpected backup error",
						zap.Reflect("error", resp.Error))
				}
				log.Info("put fine grained range",
					logutil.Key("startKey", resp.StartKey),
					logutil.Key("endKey", resp.EndKey),
				)
				rangeTree.Put(resp.StartKey, resp.EndKey, resp.Files)

				// Update progress
				progressCallBack(RegionUnit)
			}
		}

		// Step3. Backoff if needed, then repeat.
		max.mu.Lock()
		ms := max.ms
		max.mu.Unlock()
		if ms != 0 {
			log.Info("handle fine grained", zap.Int("backoffMs", ms))
			// TODO: fill a meaningful error.
			err := bo.BackoffWithMaxSleepTxnLockFast(ms, berrors.ErrUnknown)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// OnBackupResponse checks the backup resp, decides whether to retry and generate the error.
func OnBackupResponse(
	storeID uint64,
	bo *tikv.Backoffer,
	backupTS uint64,
	lockResolver *txnlock.LockResolver,
	resp *backuppb.BackupResponse,
) (*backuppb.BackupResponse, int, error) {
	log.Debug("OnBackupResponse", zap.Reflect("resp", resp))
	if resp.Error == nil {
		return resp, 0, nil
	}
	backoffMs := 0
	switch v := resp.Error.Detail.(type) {
	case *backuppb.Error_KvError:
		if lockErr := v.KvError.Locked; lockErr != nil {
			// Try to resolve lock.
			log.Warn("backup occur kv error", zap.Reflect("error", v))
			msBeforeExpired, _, err1 := lockResolver.ResolveLocks(
				bo, backupTS, []*txnlock.Lock{txnlock.NewLock(lockErr)})
			if err1 != nil {
				return nil, 0, errors.Trace(err1)
			}
			if msBeforeExpired > 0 {
				backoffMs = int(msBeforeExpired)
			}
			return nil, backoffMs, nil
		}
		// Backup should not meet error other than KeyLocked.
		log.Error("unexpect kv error", zap.Reflect("KvError", v.KvError))
		return nil, backoffMs, errors.Annotatef(berrors.ErrKVUnknown, "storeID: %d OnBackupResponse error %v", storeID, v)

	case *backuppb.Error_RegionError:
		regionErr := v.RegionError
		// Ignore following errors.
		if !(regionErr.EpochNotMatch != nil ||
			regionErr.NotLeader != nil ||
			regionErr.RegionNotFound != nil ||
			regionErr.ServerIsBusy != nil ||
			regionErr.StaleCommand != nil ||
			regionErr.StoreNotMatch != nil ||
			regionErr.ReadIndexNotReady != nil ||
			regionErr.ProposalInMergingMode != nil) {
			log.Error("unexpect region error", zap.Reflect("RegionError", regionErr))
			return nil, backoffMs, errors.Annotatef(berrors.ErrKVUnknown, "storeID: %d OnBackupResponse error %v", storeID, v)
		}
		log.Warn("backup occur region error",
			zap.Reflect("RegionError", regionErr),
			zap.Uint64("storeID", storeID))
		// TODO: a better backoff.
		backoffMs = 1000 /* 1s */
		return nil, backoffMs, nil
	case *backuppb.Error_ClusterIdError:
		log.Error("backup occur cluster ID error", zap.Reflect("error", v), zap.Uint64("storeID", storeID))
		return nil, 0, errors.Annotatef(berrors.ErrKVClusterIDMismatch, "%v on storeID: %d", resp.Error, storeID)
	default:
		// UNSAFE! TODO: use meaningful error code instead of unstructured message to find failed to write error.
		if utils.MessageIsRetryableStorageError(resp.GetError().GetMsg()) {
			log.Warn("backup occur storage error", zap.String("error", resp.GetError().GetMsg()))
			// back off 3000ms, for S3 is 99.99% available (i.e. the max outage time would less than 52.56mins per year),
			// this time would be probably enough for s3 to resume.
			return nil, 3000, nil
		}
		log.Error("backup occur unknown error", zap.String("error", resp.Error.GetMsg()), zap.Uint64("storeID", storeID))
		return nil, 0, errors.Annotatef(berrors.ErrKVUnknown, "%v on storeID: %d", resp.Error, storeID)
	}
}

func (bc *Client) handleFineGrained(
	ctx context.Context,
	bo *tikv.Backoffer,
	rg rtree.Range,
	lastBackupTS uint64,
	backupTS uint64,
	compressType backuppb.CompressionType,
	compressionLevel int32,
	rateLimit uint64,
	concurrency uint32,
	respCh chan<- *backuppb.BackupResponse,
) (int, error) {
	leader, pderr := bc.findRegionLeader(ctx, rg.StartKey)
	if pderr != nil {
		return 0, errors.Trace(pderr)
	}
	storeID := leader.GetStoreId()

	req := backuppb.BackupRequest{
		ClusterId:        bc.clusterID,
		StartKey:         rg.StartKey, // TODO: the range may cross region.
		EndKey:           rg.EndKey,
		StartVersion:     lastBackupTS,
		EndVersion:       backupTS,
		StorageBackend:   bc.backend,
		RateLimit:        rateLimit,
		Concurrency:      concurrency,
		CompressionType:  compressType,
		CompressionLevel: compressionLevel,
	}
	lockResolver := bc.mgr.GetLockResolver()
	client, err := bc.mgr.GetBackupClient(ctx, storeID)
	if err != nil {
		if berrors.Is(err, berrors.ErrFailedToConnect) {
			// When the leader store is died,
			// 20s for the default max duration before the raft election timer fires.
			log.Warn("failed to connect to store, skipping", logutil.ShortError(err), zap.Uint64("storeID", storeID))
			return 20000, nil
		}

		log.Error("fail to connect store", zap.Uint64("StoreID", storeID))
		return 0, errors.Annotatef(err, "failed to connect to store %d", storeID)
	}
	hasProgress := false
	backoffMill := 0
	err = SendBackup(
		ctx, storeID, client, req,
		// Handle responses with the same backoffer.
		func(resp *backuppb.BackupResponse) error {
			response, shouldBackoff, err1 :=
				OnBackupResponse(storeID, bo, backupTS, lockResolver, resp)
			if err1 != nil {
				return err1
			}
			if backoffMill < shouldBackoff {
				backoffMill = shouldBackoff
			}
			if response != nil {
				respCh <- response
			}
			// When meet an error, we need to set hasProgress too, in case of
			// overriding the backoffTime of original error.
			// hasProgress would be false iff there is a early io.EOF from the stream.
			hasProgress = true
			return nil
		},
		func() (backuppb.BackupClient, error) {
			log.Warn("reset the connection in handleFineGrained", zap.Uint64("storeID", storeID))
			return bc.mgr.ResetBackupClient(ctx, storeID)
		})
	if err != nil {
		if berrors.Is(err, berrors.ErrFailedToConnect) {
			// When the leader store is died,
			// 20s for the default max duration before the raft election timer fires.
			log.Warn("failed to connect to store, skipping", logutil.ShortError(err), zap.Uint64("storeID", storeID))
			return 20000, nil
		}
		log.Error("failed to send fine-grained backup", zap.Uint64("storeID", storeID), logutil.ShortError(err))
		return 0, errors.Annotatef(err, "failed to send fine-grained backup [%s, %s)",
			redact.Key(req.StartKey), redact.Key(req.EndKey))
	}

	// If no progress, backoff 10s for debouncing.
	// 10s is the default interval of stores sending a heartbeat to the PD.
	// And is the average new leader election timeout, which would be a reasonable back off time.
	if !hasProgress {
		backoffMill = 10000
	}
	return backoffMill, nil
}

// SendBackup send backup request to the given store.
// Stop receiving response if respFn returns error.
func SendBackup(
	ctx context.Context,
	storeID uint64,
	client backuppb.BackupClient,
	req backuppb.BackupRequest,
	respFn func(*backuppb.BackupResponse) error,
	resetFn func() (backuppb.BackupClient, error),
) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			fmt.Sprintf("Client.SendBackup, storeID = %d, StartKey = %s, EndKey = %s",
				storeID, redact.Key(req.StartKey), redact.Key(req.EndKey)),
			opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var errReset error
backupLoop:
	for retry := 0; retry < backupRetryTimes; retry++ {
		log.Info("try backup",
			logutil.Key("startKey", req.StartKey),
			logutil.Key("endKey", req.EndKey),
			zap.Uint64("storeID", storeID),
			zap.Int("retry time", retry),
		)
		failpoint.Inject("hint-backup-start", func(v failpoint.Value) {
			log.Info("failpoint hint-backup-start injected, " +
				"process will notify the shell.")
			if sigFile, ok := v.(string); ok {
				file, err := os.Create(sigFile)
				if err != nil {
					log.Warn("failed to create file for notifying, skipping notify", zap.Error(err))
				}
				if file != nil {
					file.Close()
				}
			}
			time.Sleep(3 * time.Second)
		})
		bcli, err := client.Backup(ctx, &req)
		failpoint.Inject("reset-retryable-error", func(val failpoint.Value) {
			if val.(bool) {
				log.Debug("failpoint reset-retryable-error injected.")
				err = status.Error(codes.Unavailable, "Unavailable error")
			}
		})
		failpoint.Inject("reset-not-retryable-error", func(val failpoint.Value) {
			if val.(bool) {
				log.Debug("failpoint reset-not-retryable-error injected.")
				err = status.Error(codes.Unknown, "Your server was haunted hence doesn't work, meow :3")
			}
		})
		if err != nil {
			if isRetryableError(err) {
				time.Sleep(3 * time.Second)
				client, errReset = resetFn()
				if errReset != nil {
					return errors.Annotatef(errReset, "failed to reset backup connection on store:%d "+
						"please check the tikv status", storeID)
				}
				continue
			}
			log.Error("fail to backup", zap.Uint64("StoreID", storeID),
				zap.Int("retry time", retry))
			return berrors.ErrFailedToConnect.Wrap(err).GenWithStack("failed to create backup stream to store %d", storeID)
		}
		defer bcli.CloseSend()

		for {
			resp, err := bcli.Recv()
			if err != nil {
				if errors.Cause(err) == io.EOF { // nolint:errorlint
					log.Info("backup streaming finish",
						zap.Uint64("StoreID", storeID),
						zap.Int("retry time", retry))
					break backupLoop
				}
				if isRetryableError(err) {
					time.Sleep(3 * time.Second)
					// current tikv is unavailable
					client, errReset = resetFn()
					if errReset != nil {
						return errors.Annotatef(errReset, "failed to reset recv connection on store:%d "+
							"please check the tikv status", storeID)
					}
					break
				}
				return berrors.ErrFailedToConnect.Wrap(err).GenWithStack("failed to connect to store: %d with retry times:%d", storeID, retry)
			}

			// TODO: handle errors in the resp.
			log.Info("range backuped",
				logutil.Key("startKey", resp.GetStartKey()),
				logutil.Key("endKey", resp.GetEndKey()))
			err = respFn(resp)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// isRetryableError represents whether we should retry reset grpc connection.
func isRetryableError(err error) bool {
	return status.Code(err) == codes.Unavailable
}
