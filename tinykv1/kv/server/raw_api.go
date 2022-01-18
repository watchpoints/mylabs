package server

import (
	"context"
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var resp kvrpcpb.RawGetResponse //返回结果
	// get storage reader.
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &resp, err
	}
	defer reader.Close()

	//API
	val, err := reader.GetCF(req.Cf, req.Key)

	//返回结果
	if err == badger.ErrKeyNotFound || val == nil || 0 == len(val) {
		log.Println("RawGet key=", req.Key, " ErrKeyNotFound err= ", err)
		resp.NotFound = true
		//记录不存在不是错误
		return &resp, nil
	} else {
		log.Println("RawGet key=", req.Key, "value ", val, "size=", len(val))
		resp.Value = val
	}

	return &resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	var resp kvrpcpb.RawPutResponse //返回结果

	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	error := server.storage.Write(req.Context, batch)
	return &resp, error
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	var resp kvrpcpb.RawDeleteResponse //返回结果

	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	error := server.storage.Write(req.Context, batch)
	return &resp, error

}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).

	var resp kvrpcpb.RawScanResponse //返回结果
	resp.Kvs = make([]*kvrpcpb.KvPair, 0)

	//抽象接口：
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &resp, err
	}
	defer reader.Close()
	//范围读
	dBIterator := reader.IterCF(req.Cf)
	defer dBIterator.Close()
	//panic: Unclosed iterator at time of Txn.Discard.
	sum := uint32(0)
	seek := req.StartKey
	limit := req.Limit
	for dBIterator.Seek(seek); dBIterator.Valid(); dBIterator.Next() {
		if limit == sum {
			break
		}
		item := dBIterator.Item()
		var temp kvrpcpb.KvPair
		temp.Key = item.Key()
		v, err := item.Value()
		if err != nil {

		} else {
			temp.Value = v
			resp.Kvs = append(resp.Kvs, &temp)
		}

		sum++
	}

	return &resp, nil
}
