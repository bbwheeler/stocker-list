package grpcstore

import (
	"context"
	"fmt"
	"time"

	tsxv1 "github.com/anomalyco/stocker-list/gen/tsx/v1"
	"google.golang.org/grpc"
)

type Store struct {
	conn *grpc.ClientConn
}

func New(ctx context.Context, address string) (*Store, error) {
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("dial stocker-store: %w", err)
	}
	return &Store{conn: conn}, nil
}

func (s *Store) GetStocks(ctx context.Context) (*tsxv1.StockerList_GetStocksClient, error) {
	client := tsxv1.NewStockerListClient(s.conn)
	stream, err := client.GetStocks(ctx)
	if err != nil {
		return nil, fmt.Errorf("get stocks stream: %w", err)
	}
	return &tsxv1.StockerList_GetStocksClient{Client: *stream}, nil
}

func (s *Store) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
