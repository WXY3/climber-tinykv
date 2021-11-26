package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var err error
	storageReader, err := server.storage.Reader(req.Context)
	val, err := storageReader.GetCF(req.Cf, req.Key)
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, err
	}
	return &kvrpcpb.RawGetResponse{Value: val, NotFound: false}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{{Data: storage.Put{Cf: req.Cf,Key: req.Key, Value: req.Value}}})
	if err != nil {
		return &kvrpcpb.RawPutResponse{}, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{{Data: storage.Delete{Cf: req.Cf, Key: req.Key}}})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var KvPairs []*kvrpcpb.KvPair
	var i uint32
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}
	dBIterator := storageReader.IterCF(req.Cf)
	for dBIterator.Seek(req.StartKey);dBIterator.Valid(); dBIterator.Next(){
		item := dBIterator.Item()
		k := item.Key()
		v, err := item.ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{}, err
		}
		KvPair := kvrpcpb.KvPair{Key: k, Value: v}
		KvPairs = append(KvPairs, &KvPair)

		i = i + 1
		if i >= req.Limit {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: KvPairs}, nil
}
