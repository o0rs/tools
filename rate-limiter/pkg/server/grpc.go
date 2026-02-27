package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"rate-limiter/pb"
	"rate-limiter/pkg/config"
	"rate-limiter/pkg/limiter"
	"rate-limiter/pkg/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// GRPCServer provides a gRPC API for rate limiting.
type GRPCServer struct {
	pb.UnimplementedRateLimitServiceServer
	cfg     *config.Config
	manager *limiter.Manager
	metrics *metrics.Metrics
	server  *grpc.Server
}

// NewGRPCServer creates a new gRPC server instance.
func NewGRPCServer(cfg *config.Config, mgr *limiter.Manager, m *metrics.Metrics) *GRPCServer {
	return &GRPCServer{
		cfg:     cfg,
		manager: mgr,
		metrics: m,
	}
}

// Start begins listening and serving gRPC requests.
func (s *GRPCServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.cfg.GRPCPort, err)
	}

	s.server = grpc.NewServer()
	pb.RegisterRateLimitServiceServer(s.server, s)

	// Enable server reflection for tools like grpcurl
	reflection.Register(s.server)

	log.Printf("[gRPC] listening on :%d", s.cfg.GRPCPort)
	return s.server.Serve(lis)
}

// Stop gracefully shuts down the gRPC server.
func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// Allow implements the RateLimitService.Allow RPC.
func (s *GRPCServer) Allow(ctx context.Context, req *pb.AllowRequest) (*pb.AllowResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	tokens := int(req.GetTokens())
	if tokens <= 0 {
		tokens = 1
	}

	algoStr := req.GetAlgorithm()
	if algoStr == "" {
		algoStr = s.cfg.DefaultAlgorithm
	}
	algo := limiter.ParseAlgorithm(algoStr)

	start := time.Now()
	result := s.manager.AllowWithAlgo(req.GetKey(), algo, tokens)
	duration := time.Since(start).Seconds()

	// Record Prometheus metrics
	resultLabel := "allowed"
	if !result.Allowed {
		resultLabel = "denied"
	}
	s.metrics.RecordRequest(req.GetKey(), algoStr, resultLabel)
	s.metrics.ObserveDuration(algoStr, duration)
	s.metrics.SetRemaining(req.GetKey(), algoStr, float64(result.Remaining))

	msg := "request allowed"
	if !result.Allowed {
		msg = "rate limit exceeded"
	}

	return &pb.AllowResponse{
		Allowed:      result.Allowed,
		Remaining:    int32(result.Remaining),
		RetryAfterMs: float64(result.RetryAfter.Milliseconds()),
		Message:      msg,
	}, nil
}
