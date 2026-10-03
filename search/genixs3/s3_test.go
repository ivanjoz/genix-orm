package genixs3

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ivanjoz/genix-orm/db"
	genix "github.com/ivanjoz/genix-search/genix-s3"
)

// TestOnS3 runs the backend through New against the bucket in GENIX_S3_TEST_BUCKET (an S3 Express
// directory bucket or a general purpose one), with the default AWS credentials. It writes under
// genix-orm-it/<nanos>/ and deletes it at the end.
func TestOnS3(t *testing.T) {
	bucket := os.Getenv("GENIX_S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("GENIX_S3_TEST_BUCKET is not set")
	}
	ctx := context.Background()

	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig)
	root := fmt.Sprintf("genix-orm-it/%d/", time.Now().UnixNano())
	t.Cleanup(func() { deletePrefix(t, client, bucket, root) })

	b, err := New(ctx, Config{Bucket: bucket, S3: client, Root: root, MinIndexDocs: 3, Compact: genix.CompactSync})
	if err != nil {
		t.Fatal(err)
	}

	puts := []db.TextDoc{
		{ID: 1, Text: "Polo Nike"},
		{ID: 2, Text: "Gorra Puma"},
		{ID: 3, Text: "Polo Lacoste"},
		{ID: 4, Text: "Casaca Adidas"},
	}
	if err := b.Apply(ctx, "products", "p1", puts, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(ctx, "products", "p1", []db.TextDoc{{ID: 2, Text: "Gorra Polo"}}, []int32{3}); err != nil {
		t.Fatal(err)
	}

	hits, err := b.Search(ctx, "products", "p1", "polo", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int32, len(hits))
	for i, hit := range hits {
		ids[i] = hit.ID
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []int32{1, 2}) {
		t.Errorf("polo = %v, want [1 2]", ids)
	}

	if err := b.Drop(ctx, "products", "p1"); err != nil {
		t.Fatal(err)
	}
	if hits, err := b.Search(ctx, "products", "p1", "polo", 10, 0); err != nil || len(hits) != 0 {
		t.Errorf("after Drop: %v %v", hits, err)
	}
}

func deletePrefix(t *testing.T, client *s3.Client, bucket, prefix string) {
	ctx := context.Background()
	input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)}
	for {
		out, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			t.Logf("cleanup list: %v", err)
			return
		}
		if len(out.Contents) > 0 {
			objects := make([]types.ObjectIdentifier, len(out.Contents))
			for i, object := range out.Contents {
				objects[i] = types.ObjectIdentifier{Key: object.Key}
			}
			if _, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects}}); err != nil {
				t.Logf("cleanup delete: %v", err)
			}
		}
		if out.NextContinuationToken == nil {
			return
		}
		input.ContinuationToken = out.NextContinuationToken
	}
}
