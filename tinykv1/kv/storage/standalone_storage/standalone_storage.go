package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func getTestOptions(dir string) badger.Options {
	opt := badger.DefaultOptions
	opt.MaxTableSize = 4 << 15 // Force more compaction.
	opt.LevelOneSize = 4 << 15 // Force more compaction.
	opt.Dir = dir
	opt.ValueDir = dir
	opt.SyncWrites = false
	return opt
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	BadgerDB *badger.DB
	Opts     badger.Options
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	log.Println("NewStandAloneStorage  ")
	var standAloneStorage StandAloneStorage
	conf.DBPath = "/root/money/golang/src/tinykv/bin/badger"
	standAloneStorage.Opts = badger.DefaultOptions
	standAloneStorage.Opts.Dir = conf.DBPath
	standAloneStorage.Opts.ValueDir = conf.DBPath

	standAloneStorage.BadgerDB = nil //还没有打开连接

	return &standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1)
	log.Println("Start()")
	var err error
	s.BadgerDB, err = badger.Open(s.Opts)
	if err != nil {
		log.Fatal("Open failed:", err, "path=", s.Opts.Dir)
	}

	//defer s.BadgerDB.Close() ,
	//er用于资源的释放，会在函数返回之前进行调用
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	log.Println("Stop ")
	err := s.BadgerDB.Close()
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.BadgerDB.NewTransaction(false)
	reader := NewStandaloneReader(txn)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	var err error
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			cfKey := engine_util.KeyWithCF(m.Cf(), m.Key())
			log.Println("Write key=", cfKey, " value=", m.Value())
			err = engine_util.PutCF(s.BadgerDB, m.Cf(), m.Key(), m.Value())

		case storage.Delete:
			//delete := m.Data.(storage.Delete)
			cfKey := engine_util.KeyWithCF(m.Cf(), m.Key())
			log.Println("Delete key=", cfKey)
			err = engine_util.DeleteCF(s.BadgerDB, m.Cf(), m.Key())

		}
	}
	return err

	/**
	var err error
	txn := s.BadgerDB.NewTransaction(true)

	defer txn.Discard()
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			//put := m.Data.(storage.Put)
			log.Println("Write key=", m.CfKey(), " value=", m.Value())
			err = txn.Set([]byte(m.CfKey()), m.Value())
			if err != nil {
				return err
			}

		case storage.Delete:
			//delete := m.Data.(storage.Delete)
			log.Println("Delete key=", m.CfKey())
			err = txn.Delete([]byte(m.CfKey()))
			if err != nil {
				return err
			}

		}
	}
	txn.Commit()
	return err
	**/
}

type StandaloneReader struct {
	txn *badger.Txn
}

func NewStandaloneReader(txn *badger.Txn) *StandaloneReader {
	return &StandaloneReader{
		txn: txn,
	}
}

func (r *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	// Your Code Here (1).

	item, err := r.txn.Get(engine_util.KeyWithCF(cf, key))
	//查找的key 不存在不是错误
	if err == badger.ErrKeyNotFound {
		log.Println("GetCF key=", key, "err =", err)
		return nil, nil
	}
	if err != nil {
		log.Println("GetCF key=", key, "err =", err)
		return nil, err
	}

	val, err1 := item.Value()
	if err1 != nil {
		log.Println("GetCF key=", key, "err =", err1)
		return nil, err
	}
	return val, err1
}

//
func (r *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	// Your Code Here (1).
	//全部遍历
	iterator := engine_util.NewCFIterator(cf, r.txn)
	return iterator
}

func (r *StandaloneReader) Close() {
	// Your Code Here (1).

	r.txn.Discard()
	//Don’t forget to call Discard() for badger.Txn and close all iterators before discarding.

}
