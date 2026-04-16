package vectorstore

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type testCollectionsServer struct {
	pb.UnimplementedCollectionsServer

	mu         sync.Mutex
	listResp   *pb.ListCollectionsResponse
	listErr    error
	createErr  error
	createReqs []*pb.CreateCollection
}

func (s *testCollectionsServer) List(context.Context, *pb.ListCollectionsRequest) (*pb.ListCollectionsResponse, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	if s.listResp == nil {
		return &pb.ListCollectionsResponse{}, nil
	}
	return s.listResp, nil
}

func (s *testCollectionsServer) Create(_ context.Context, req *pb.CreateCollection) (*pb.CollectionOperationResponse, error) {
	s.mu.Lock()
	s.createReqs = append(s.createReqs, req)
	s.mu.Unlock()
	if s.createErr != nil {
		return nil, s.createErr
	}
	return &pb.CollectionOperationResponse{}, nil
}

type testPointsServer struct {
	pb.UnimplementedPointsServer

	mu         sync.Mutex
	upsertErr  error
	searchErr  error
	searchResp *pb.SearchResponse
	upsertReqs []*pb.UpsertPoints
	searchReqs []*pb.SearchPoints
}

func (s *testPointsServer) Upsert(_ context.Context, req *pb.UpsertPoints) (*pb.PointsOperationResponse, error) {
	s.mu.Lock()
	s.upsertReqs = append(s.upsertReqs, req)
	s.mu.Unlock()
	if s.upsertErr != nil {
		return nil, s.upsertErr
	}
	return &pb.PointsOperationResponse{}, nil
}

func (s *testPointsServer) Search(_ context.Context, req *pb.SearchPoints) (*pb.SearchResponse, error) {
	s.mu.Lock()
	s.searchReqs = append(s.searchReqs, req)
	s.mu.Unlock()
	if s.searchErr != nil {
		return nil, s.searchErr
	}
	if s.searchResp == nil {
		return &pb.SearchResponse{}, nil
	}
	return s.searchResp, nil
}

func newTestStore(t *testing.T, collectionsSrv *testCollectionsServer, pointsSrv *testPointsServer) *Store {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterCollectionsServer(server, collectionsSrv)
	pb.RegisterPointsServer(server, pointsSrv)

	go func() {
		_ = server.Serve(lis)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &Store{
		client:     pb.NewPointsClient(conn),
		collection: pb.NewCollectionsClient(conn),
		colName:    defaultCollection,
	}
}

func TestEnsureCollection(t *testing.T) {
	t.Parallel()

	t.Run("existing collection skips create", func(t *testing.T) {
		t.Parallel()
		colSrv := &testCollectionsServer{
			listResp: &pb.ListCollectionsResponse{Collections: []*pb.CollectionDescription{{Name: defaultCollection}}},
		}
		store := newTestStore(t, colSrv, &testPointsServer{})

		if err := store.EnsureCollection(context.Background(), 1024); err != nil {
			t.Fatalf("EnsureCollection() unexpected error: %v", err)
		}
		if len(colSrv.createReqs) != 0 {
			t.Fatalf("create called %d times, want 0", len(colSrv.createReqs))
		}
	})

	t.Run("missing collection creates with requested dimension and cosine distance", func(t *testing.T) {
		t.Parallel()
		colSrv := &testCollectionsServer{
			listResp: &pb.ListCollectionsResponse{Collections: []*pb.CollectionDescription{{Name: "other"}}},
		}
		store := newTestStore(t, colSrv, &testPointsServer{})

		if err := store.EnsureCollection(context.Background(), 2048); err != nil {
			t.Fatalf("EnsureCollection() unexpected error: %v", err)
		}
		if len(colSrv.createReqs) != 1 {
			t.Fatalf("create called %d times, want 1", len(colSrv.createReqs))
		}

		req := colSrv.createReqs[0]
		if req.GetCollectionName() != defaultCollection {
			t.Fatalf("CollectionName = %q, want %q", req.GetCollectionName(), defaultCollection)
		}
		params := req.GetVectorsConfig().GetParams()
		if params == nil {
			t.Fatal("VectorsConfig params is nil")
		}
		if params.GetSize() != 2048 {
			t.Fatalf("Vector size = %d, want 2048", params.GetSize())
		}
		if params.GetDistance() != pb.Distance_Cosine {
			t.Fatalf("Distance = %v, want %v", params.GetDistance(), pb.Distance_Cosine)
		}
	})

	t.Run("list error is wrapped", func(t *testing.T) {
		t.Parallel()
		colSrv := &testCollectionsServer{listErr: errors.New("list boom")}
		store := newTestStore(t, colSrv, &testPointsServer{})

		err := store.EnsureCollection(context.Background(), 1024)
		if err == nil {
			t.Fatal("EnsureCollection() returned nil error, want wrapped error")
		}
		if !strings.Contains(err.Error(), "failed to list collections") {
			t.Fatalf("error = %q, want list context", err.Error())
		}
	})

	t.Run("create error is wrapped", func(t *testing.T) {
		t.Parallel()
		colSrv := &testCollectionsServer{
			listResp:  &pb.ListCollectionsResponse{Collections: []*pb.CollectionDescription{{Name: "other"}}},
			createErr: errors.New("create boom"),
		}
		store := newTestStore(t, colSrv, &testPointsServer{})

		err := store.EnsureCollection(context.Background(), 1024)
		if err == nil {
			t.Fatal("EnsureCollection() returned nil error, want wrapped create error")
		}
		if !strings.Contains(err.Error(), "failed to create collection") {
			t.Fatalf("error = %q, want create context", err.Error())
		}
	})
}

func TestUpsert(t *testing.T) {
	t.Parallel()

	t.Run("empty input does not call RPC", func(t *testing.T) {
		t.Parallel()
		ptSrv := &testPointsServer{}
		store := newTestStore(t, &testCollectionsServer{}, ptSrv)

		if err := store.Upsert(context.Background(), nil); err != nil {
			t.Fatalf("Upsert(nil) unexpected error: %v", err)
		}
		if len(ptSrv.upsertReqs) != 0 {
			t.Fatalf("Upsert RPC called %d times, want 0", len(ptSrv.upsertReqs))
		}
	})

	t.Run("maps point payload and uses deterministic UUID", func(t *testing.T) {
		t.Parallel()
		ptSrv := &testPointsServer{}
		store := newTestStore(t, &testCollectionsServer{}, ptSrv)

		point := Point{
			ChunkID:    "https://example.com#chunk-3",
			Vector:     []float32{0.1, 0.2},
			URL:        "https://example.com",
			Title:      "Example",
			ChunkIndex: 3,
			ChunkText:  "chunk text",
			WorkerID:   "worker-1",
		}

		if err := store.Upsert(context.Background(), []Point{point}); err != nil {
			t.Fatalf("Upsert() unexpected error: %v", err)
		}
		if err := store.Upsert(context.Background(), []Point{point}); err != nil {
			t.Fatalf("second Upsert() unexpected error: %v", err)
		}
		if len(ptSrv.upsertReqs) != 2 {
			t.Fatalf("Upsert RPC calls = %d, want 2", len(ptSrv.upsertReqs))
		}

		first := ptSrv.upsertReqs[0].GetPoints()[0]
		second := ptSrv.upsertReqs[1].GetPoints()[0]
		if first.GetId().GetUuid() == "" {
			t.Fatal("UUID is empty")
		}
		if first.GetId().GetUuid() != second.GetId().GetUuid() {
			t.Fatalf("UUID is not deterministic: first=%q second=%q", first.GetId().GetUuid(), second.GetId().GetUuid())
		}
		if got := first.GetPayload()["url"].GetStringValue(); got != point.URL {
			t.Fatalf("payload[url] = %q, want %q", got, point.URL)
		}
		if got := first.GetPayload()["title"].GetStringValue(); got != point.Title {
			t.Fatalf("payload[title] = %q, want %q", got, point.Title)
		}
		if got := first.GetPayload()["chunk_index"].GetIntegerValue(); got != int64(point.ChunkIndex) {
			t.Fatalf("payload[chunk_index] = %d, want %d", got, point.ChunkIndex)
		}
		if got := first.GetPayload()["text"].GetStringValue(); got != point.ChunkText {
			t.Fatalf("payload[text] = %q, want %q", got, point.ChunkText)
		}
		if got := first.GetPayload()["worker_id"].GetStringValue(); got != point.WorkerID {
			t.Fatalf("payload[worker_id] = %q, want %q", got, point.WorkerID)
		}
	})

	t.Run("RPC error is returned", func(t *testing.T) {
		t.Parallel()
		ptSrv := &testPointsServer{upsertErr: errors.New("upsert boom")}
		store := newTestStore(t, &testCollectionsServer{}, ptSrv)

		err := store.Upsert(context.Background(), []Point{{
			ChunkID:    "id-1",
			Vector:     []float32{0.1},
			URL:        "https://example.com",
			Title:      "Title",
			ChunkIndex: 0,
			ChunkText:  "text",
			WorkerID:   "worker-1",
		}})
		if err == nil {
			t.Fatal("Upsert() returned nil error, want RPC error")
		}
		if !strings.Contains(err.Error(), "upsert boom") {
			t.Fatalf("error = %q, want upstream RPC error details", err.Error())
		}
	})
}

func TestSearch(t *testing.T) {
	t.Parallel()

	t.Run("forwards request and returns scored points", func(t *testing.T) {
		t.Parallel()

		ptSrv := &testPointsServer{
			searchResp: &pb.SearchResponse{Result: []*pb.ScoredPoint{{Score: 0.77, Payload: map[string]*pb.Value{"url": strVal("https://example.com")}}}},
		}
		store := newTestStore(t, &testCollectionsServer{}, ptSrv)

		got, err := store.Search(context.Background(), []float32{0.3, 0.4}, 5)
		if err != nil {
			t.Fatalf("Search() unexpected error: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("Search() result length = %d, want 1", len(got))
		}
		if got[0].GetPayload()["url"].GetStringValue() != "https://example.com" {
			t.Fatalf("Search() payload[url] = %q, want %q", got[0].GetPayload()["url"].GetStringValue(), "https://example.com")
		}

		if len(ptSrv.searchReqs) != 1 {
			t.Fatalf("Search RPC calls = %d, want 1", len(ptSrv.searchReqs))
		}
		req := ptSrv.searchReqs[0]
		if req.GetCollectionName() != defaultCollection {
			t.Fatalf("CollectionName = %q, want %q", req.GetCollectionName(), defaultCollection)
		}
		if req.GetLimit() != 5 {
			t.Fatalf("Limit = %d, want 5", req.GetLimit())
		}
		if len(req.GetVector()) != 2 || req.GetVector()[0] != 0.3 || req.GetVector()[1] != 0.4 {
			t.Fatalf("Vector = %v, want [0.3 0.4]", req.GetVector())
		}
		if !req.GetWithPayload().GetEnable() {
			t.Fatal("WithPayload.Enable = false, want true")
		}
	})

	t.Run("RPC error is returned", func(t *testing.T) {
		t.Parallel()

		ptSrv := &testPointsServer{searchErr: errors.New("search boom")}
		store := newTestStore(t, &testCollectionsServer{}, ptSrv)

		_, err := store.Search(context.Background(), []float32{0.3, 0.4}, 5)
		if err == nil {
			t.Fatal("Search() returned nil error, want RPC error")
		}
		if !strings.Contains(err.Error(), "search boom") {
			t.Fatalf("error = %q, want upstream RPC error details", err.Error())
		}
	})
}
