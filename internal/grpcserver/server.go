// Package grpcserver implements the tsx.v1.CompanyService gRPC API on top
// of the Postgres-backed repository.
package grpcserver

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/anomalyco/stocker-list/internal/db"

	tsxv1 "github.com/anomalyco/stocker-list/gen/tsx/v1"
)

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

type Server struct {
	tsxv1.UnimplementedCompanyServiceServer

	repo *db.Repository
	log  *slog.Logger
}

func New(repo *db.Repository, log *slog.Logger) *Server {
	return &Server{repo: repo, log: log}
}

func (s *Server) GetCompany(ctx context.Context, req *tsxv1.GetCompanyRequest) (*tsxv1.GetCompanyResponse, error) {
	if req.GetSymbol() == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}

	c, err := s.repo.GetBySymbol(ctx, req.GetSymbol())
	if err != nil {
		if err == db.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "no company found for symbol %q", req.GetSymbol())
		}
		s.log.Error("GetCompany failed", "symbol", req.GetSymbol(), "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	return &tsxv1.GetCompanyResponse{Company: toProto(c)}, nil
}

func (s *Server) ListCompanies(ctx context.Context, req *tsxv1.ListCompaniesRequest) (*tsxv1.ListCompaniesResponse, error) {
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}

	companies, err := s.repo.List(ctx, req.GetPageToken(), pageSize)
	if err != nil {
		s.log.Error("ListCompanies failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	total, err := s.repo.Count(ctx)
	if err != nil {
		s.log.Error("Count failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	resp := &tsxv1.ListCompaniesResponse{
		Companies:  make([]*tsxv1.Company, 0, len(companies)),
		TotalCount: int32(total),
	}
	for i := range companies {
		resp.Companies = append(resp.Companies, toProto(&companies[i]))
	}
	// The page_token is simply the last symbol returned (keyset pagination).
	// An empty token signals no further pages.
	if len(companies) == pageSize {
		resp.NextPageToken = companies[len(companies)-1].Symbol
	}

	return resp, nil
}

func toProto(c *db.Company) *tsxv1.Company {
	return &tsxv1.Company{
		Symbol:   c.Symbol,
		Name:     c.Name,
		Exchange: c.Exchange,
		Currency: c.Currency,
	}
}
