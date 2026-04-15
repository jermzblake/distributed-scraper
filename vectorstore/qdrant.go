package vectorstore

import (
	"context"
	"fmt"
	"crypto/tls"

	"github.com/google/uuid"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultCollection = "scraped_docs"

// Store wraps he Qdrant gRPC client
type Store struct {
	client 			pb.PointsClient
	collection 	pb.CollectionsClient
	colName 		string
}

// Point is what we upsert - one chunk of text with its embedding and metadata.
type Point struct {
	// ChunkID is a stable ID: we derive it from URL + chunk index so re-scraping
	// the same URL overwrites existing points rather than creating duplicates.
	ChunkID string
	Vector []float32
	URL string
	Title string
	ChunkIndex int
	ChunkText string
	WorkerID string
}

func New(addr string, useTLS bool) (*Store, error) {
	var opts []grpc.DialOption
	if useTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Qdrant: %w", err)
	}

	return &Store{
		client: pb.NewPointsClient(conn),
		collection: pb.NewCollectionsClient(conn),
		colName: defaultCollection,
	}, nil
}

// EnsureCollection creates the collection if it doesn't exist.
func (s *Store) EnsureCollection(ctx context.Context, vectorDim uint64) error {
	// Check if collection already exists
	resp, err := s.collection.List(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	for _, col := range resp.Collections {
		if col.Name == s.colName {
			return nil	// Already exists
		}
	}

	// Create collection if it doesn't exist
	_, err = s.collection.Create(ctx, &pb.CreateCollection{
		CollectionName: s.colName,
		VectorsConfig: &pb.VectorsConfig{
			Config: &pb.VectorsConfig_Params{
				Params: &pb.VectorParams{
					Size:     vectorDim,
					Distance: pb.Distance_Cosine, // Standard for text embeddings
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	return nil
}

// Upsert stores a batch of points.
func (s *Store) Upsert(ctx context.Context, points []Point) error {
	if len(points) == 0 {
		return nil
	}

	var pbPoints []*pb.PointStruct

	for _, p := range points {
		// Qdrant requires UUID-format point IDs.
		// We generate a deterministic UUID from the chunk ID string
		// so the same chunk always gets the same UUID.
		uid := uuid.NewSHA1(uuid.NameSpaceURL, []byte(p.ChunkID))

		pbPoints = append(pbPoints, &pb.PointStruct{
			Id: &pb.PointId{
				PointIdOptions: &pb.PointId_Uuid{
					Uuid: uid.String(),
				},
			},
			Vectors: &pb.Vectors{
				VectorsOptions: &pb.Vectors_Vector{
					Vector: &pb.Vector{
						Data: p.Vector,
					},
				},
			},
			Payload: map[string]*pb.Value{
				"url":         strVal(p.URL),
				"title":       strVal(p.Title),
				"chunk_index": intVal(int64(p.ChunkIndex)),
				"text":        strVal(p.ChunkText),
				"worker_id":   strVal(p.WorkerID),
			},
		})
	}

	waitUpsert := true
	_, err := s.client.Upsert(ctx, &pb.UpsertPoints{
		CollectionName: s.colName,
		Points:        pbPoints,
		Wait:          &waitUpsert,
	})
	return err
}

// Search performs a nearest-neighbor search.
func (s *Store) Search(ctx context.Context, vector []float32, limit uint64) ([]*pb.ScoredPoint, error) {
	resp, err := s.client.Search(ctx, &pb.SearchPoints{
		CollectionName: s.colName,
		Vector:         vector,
		Limit:          limit,
		WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
	})
	if err != nil {
		return nil, err
	}
	return resp.Result, nil
}

func strVal(s string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_StringValue{StringValue: s}}
}
func intVal(i int64) *pb.Value {
	return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: i}}
}