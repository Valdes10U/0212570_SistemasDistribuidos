package server

import (
	"context"

	api "github.com/Valdes10U/0212570_SistemasDistribuidos/api/v1"
	"github.com/Valdes10U/0212570_SistemasDistribuidos/log"
)

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*log.Log
}

func newgrpcServer(commitlog *log.Log) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Log: commitlog,
	}
	return srv, nil
}
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.Log.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	offset := req.Offset
	record, err := s.Log.Read(offset)
	if err != nil {
		return nil, api.ErrOffsetOutOfRange{Offset: offset}
	}
	response := &api.ConsumeResponse{
		Record: record,
	}
	return response, nil
}
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
