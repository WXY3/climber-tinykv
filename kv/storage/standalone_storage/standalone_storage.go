package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	dbEngine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"

	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)

	engine := engine_util.NewEngines(raftDB, kvDB, kvPath, raftPath)
	return &StandAloneStorage{
		dbEngine: engine,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.dbEngine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.dbEngine.Kv.NewTransaction(false)
	return NewStandAloneStorageReader(*txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, x := range batch {
		switch x.Data.(type) {
		case storage.Put:
			engine_util.PutCF(s.dbEngine.Kv, x.Cf(), x.Key(), x.Value())
		case storage.Delete:
			engine_util.DeleteCF(s.dbEngine.Kv, x.Cf(), x.Key())
		default:
			return errors.New("storage modify type not Put or Delete")
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	bTxn badger.Txn
}

func NewStandAloneStorageReader(bTxn badger.Txn) StandAloneStorageReader {
	return StandAloneStorageReader{bTxn: bTxn}
}

func (r StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(&r.bTxn, cf, key)
	if val == nil {
		return nil, nil
	}
	return val, err
}

func (r StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, &r.bTxn)
}

func (r StandAloneStorageReader) Close() {
	r.Close()
}
