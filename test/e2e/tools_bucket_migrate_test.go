// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestToolsBucketMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping migrate e2e test in short mode")
	}

	t.Parallel()

	e, err := e2e.NewDockerEnvironment("bucket-migrate")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// Create temporary directories for source and destination storage
	sourceDir := filepath.Join(e.SharedDir(), "source-storage")
	destDir := filepath.Join(e.SharedDir(), "dest-storage")
	testutil.Ok(t, os.MkdirAll(sourceDir, 0755))
	testutil.Ok(t, os.MkdirAll(destDir, 0755))

	logger := log.NewLogfmtLogger(os.Stderr)

	// Create filesystem bucket clients
	sourceBkt, err := filesystem.NewBucket(sourceDir)
	testutil.Ok(t, err)
	defer runutil.CloseWithLogOnErr(logger, sourceBkt, "source bucket")

	destBkt, err := filesystem.NewBucket(destDir)
	testutil.Ok(t, err)
	defer runutil.CloseWithLogOnErr(logger, destBkt, "dest bucket")

	// Test cases
	t.Run("TestCutoffDatesRespected", func(t *testing.T) {
		testCutoffDatesRespected(t, e, sourceBkt, destBkt, sourceDir, destDir)
	})

	t.Run("TestDeletionMarkSet", func(t *testing.T) {
		testDeletionMarkSet(t, e, sourceBkt, destBkt, sourceDir, destDir)
	})

	t.Run("TestDryRunMode", func(t *testing.T) {
		testDryRunMode(t, e, sourceBkt, destBkt, sourceDir, destDir)
	})

	t.Run("TestExcludeDeletionMarks", func(t *testing.T) {
		testExcludeDeletionMarks(t, e, sourceBkt, destBkt, sourceDir, destDir)
	})

	t.Run("TestSelectorLabels", func(t *testing.T) {
		testSelectorLabels(t, e, sourceBkt, destBkt, sourceDir, destDir)
	})
}

// TestToolsBucketMigrateConcurrency tests migration performance with many blocks
func TestToolsBucketMigrateConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	t.Parallel()

	e, err := e2e.NewDockerEnvironment("migrate-perf")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// Create temporary directories
	sourceDir := filepath.Join(e.SharedDir(), "perf-source-storage")
	destDir := filepath.Join(e.SharedDir(), "perf-dest-storage")
	testutil.Ok(t, os.MkdirAll(sourceDir, 0755))
	testutil.Ok(t, os.MkdirAll(destDir, 0755))

	logger := log.NewLogfmtLogger(os.Stderr)

	sourceBkt, err := filesystem.NewBucket(sourceDir)
	testutil.Ok(t, err)
	defer runutil.CloseWithLogOnErr(logger, sourceBkt, "source bucket")

	destBkt, err := filesystem.NewBucket(destDir)
	testutil.Ok(t, err)
	defer runutil.CloseWithLogOnErr(logger, destBkt, "dest bucket")

	ctx := context.Background()

	// Clean up buckets
	cleanupBucket(ctx, t, sourceBkt)
	cleanupBucket(ctx, t, destBkt)

	// Create many test blocks (scaled down for practicality)
	numBlocks := 50
	blockIDs := make([]ulid.ULID, numBlocks)

	t.Logf("Creating %d test blocks...", numBlocks)
	startTime := time.Now()

	for i := 0; i < numBlocks; i++ {
		// Create blocks that are 2-5 days old (all should be migrated with 24h cutoff)
		blockAge := time.Duration(48+i) * time.Hour
		blockTime := time.Now().Add(-blockAge)

		blockIDs[i] = createTestBlock(t, ctx, sourceBkt, blockTime, labels.FromStrings(
			"env", "perf-test",
			"block_id", fmt.Sprintf("block-%d", i),
		))

		if i%10 == 0 {
			t.Logf("Created %d/%d blocks", i+1, numBlocks)
		}
	}

	creationTime := time.Since(startTime)
	t.Logf("Created %d blocks in %v", numBlocks, creationTime)

	// Test different concurrency levels
	concurrencyLevels := []int{1, 5, 10}

	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(t *testing.T) {
			// Clean destination for each test
			cleanupBucket(ctx, t, destBkt)

			t.Logf("Testing migration with concurrency=%d", concurrency)

			migrationStart := time.Now()
			runMigrationCommand(t, e, sourceDir, destDir,
				"--older-than=24h",
				fmt.Sprintf("--concurrency=%d", concurrency))
			migrationTime := time.Since(migrationStart)

			t.Logf("Migration with concurrency=%d completed in %v", concurrency, migrationTime)

			// Verify all blocks were migrated
			migratedCount := 0
			for _, blockID := range blockIDs {
				exists, err := destBkt.Exists(ctx, filepath.Join(blockID.String(), metadata.MetaFilename))
				testutil.Ok(t, err)
				if exists {
					migratedCount++
				}
			}

			testutil.Equals(t, numBlocks, migratedCount, "All blocks should be migrated with concurrency=%d", concurrency)

			// Calculate throughput
			blocksPerSecond := float64(numBlocks) / migrationTime.Seconds()
			t.Logf("Throughput: %.2f blocks/second with concurrency=%d", blocksPerSecond, concurrency)
		})
	}
}

// testCutoffDatesRespected verifies that only blocks older than the cutoff are migrated
func testCutoffDatesRespected(t *testing.T, e e2e.Environment, sourceBkt, destBkt objstore.Bucket, sourceDir, destDir string) {
	ctx := context.Background()

	// Clean up buckets
	cleanupBucket(ctx, t, sourceBkt)
	cleanupBucket(ctx, t, destBkt)

	// Create test blocks with different ages
	now := time.Now()
	oldBlockID := createTestBlock(t, ctx, sourceBkt, now.Add(-72*time.Hour), labels.FromStrings("env", "prod")) // 3 days old
	newBlockID := createTestBlock(t, ctx, sourceBkt, now.Add(-12*time.Hour), labels.FromStrings("env", "prod")) // 12 hours old

	// Run migration with 48h cutoff (should only migrate the 3-day-old block)
	runMigrationCommand(t, e, sourceDir, destDir, "--older-than=48h")

	// Verify only the old block was migrated
	verifyBlockExists(t, ctx, destBkt, oldBlockID, true)  // Should exist
	verifyBlockExists(t, ctx, destBkt, newBlockID, false) // Should not exist

	// Verify deletion mark was set on the migrated block
	verifyDeletionMark(t, ctx, sourceBkt, oldBlockID, true)  // Should have deletion mark
	verifyDeletionMark(t, ctx, sourceBkt, newBlockID, false) // Should not have deletion mark
}

// testDeletionMarkSet verifies that source blocks get deletion marks after migration
func testDeletionMarkSet(t *testing.T, e e2e.Environment, sourceBkt, destBkt objstore.Bucket, sourceDir, destDir string) {
	ctx := context.Background()

	// Clean up buckets
	cleanupBucket(ctx, t, sourceBkt)
	cleanupBucket(ctx, t, destBkt)

	// Create test block
	blockID := createTestBlock(t, ctx, sourceBkt, time.Now().Add(-72*time.Hour), labels.FromStrings("env", "test"))

	// Verify no deletion mark initially
	verifyDeletionMark(t, ctx, sourceBkt, blockID, false)

	// Run migration
	runMigrationCommand(t, e, sourceDir, destDir, "--older-than=24h")

	// Verify deletion mark was created after migration
	verifyDeletionMark(t, ctx, sourceBkt, blockID, true)

	// Verify block exists in destination
	verifyBlockExists(t, ctx, destBkt, blockID, true)
}

// testDryRunMode verifies that dry-run doesn't actually migrate blocks
func testDryRunMode(t *testing.T, e e2e.Environment, sourceBkt, destBkt objstore.Bucket, sourceDir, destDir string) {
	ctx := context.Background()

	// Clean up buckets
	cleanupBucket(ctx, t, sourceBkt)
	cleanupBucket(ctx, t, destBkt)

	// Create test block
	blockID := createTestBlock(t, ctx, sourceBkt, time.Now().Add(-72*time.Hour), labels.FromStrings("env", "dryrun"))

	// Run migration in dry-run mode
	runMigrationCommand(t, e, sourceDir, destDir, "--older-than=24h", "--dry-run")

	// Verify block was NOT migrated
	verifyBlockExists(t, ctx, destBkt, blockID, false)

	// Verify no deletion mark was created
	verifyDeletionMark(t, ctx, sourceBkt, blockID, false)
}

// testExcludeDeletionMarks verifies that blocks with deletion marks are excluded
func testExcludeDeletionMarks(t *testing.T, e e2e.Environment, sourceBkt, destBkt objstore.Bucket, sourceDir, destDir string) {
	ctx := context.Background()

	// Clean up buckets
	cleanupBucket(ctx, t, sourceBkt)
	cleanupBucket(ctx, t, destBkt)

	// Create test block and immediately mark it for deletion
	blockID := createTestBlock(t, ctx, sourceBkt, time.Now().Add(-72*time.Hour), labels.FromStrings("env", "marked"))
	createDeletionMark(t, ctx, sourceBkt, blockID)

	// Run migration (should exclude the marked block)
	runMigrationCommand(t, e, sourceDir, destDir, "--older-than=24h")

	// Verify block was NOT migrated (because it's already marked for deletion)
	verifyBlockExists(t, ctx, destBkt, blockID, false)
}

// testSelectorLabels verifies that label selectors work correctly
func testSelectorLabels(t *testing.T, e e2e.Environment, sourceBkt, destBkt objstore.Bucket, sourceDir, destDir string) {
	ctx := context.Background()

	// Clean up buckets
	cleanupBucket(ctx, t, sourceBkt)
	cleanupBucket(ctx, t, destBkt)

	// Create test blocks with different labels
	prodBlockID := createTestBlock(t, ctx, sourceBkt, time.Now().Add(-72*time.Hour), labels.FromStrings("env", "prod"))
	testBlockID := createTestBlock(t, ctx, sourceBkt, time.Now().Add(-72*time.Hour), labels.FromStrings("env", "test"))

	// Create selector config that only matches prod environment
	selectorConfig := `
- action: keep
  source_labels: [env]
  regex: prod
`
	selectorFile := filepath.Join(e.SharedDir(), "selector.yaml")
	testutil.Ok(t, os.WriteFile(selectorFile, []byte(selectorConfig), 0644))

	// Run migration with selector
	runMigrationCommand(t, e, sourceDir, destDir, "--older-than=24h", "--selector.relabel-config-file="+selectorFile)

	// Verify only the prod block was migrated
	verifyBlockExists(t, ctx, destBkt, prodBlockID, true)  // Should exist
	verifyBlockExists(t, ctx, destBkt, testBlockID, false) // Should not exist
}

// Helper functions

func cleanupBucket(ctx context.Context, t *testing.T, bkt objstore.Bucket) {
	// Delete all objects in bucket
	err := bkt.Iter(ctx, "", func(name string) error {
		return bkt.Delete(ctx, name)
	})
	testutil.Ok(t, err)
}

func createTestBlock(t *testing.T, ctx context.Context, bkt objstore.Bucket, minTime time.Time, lbls labels.Labels) ulid.ULID {
	blockID := ulid.MustNew(ulid.Timestamp(minTime), nil)

	// Create block metadata
	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    blockID,
			MinTime: timestamp.FromTime(minTime),
			MaxTime: timestamp.FromTime(minTime.Add(2 * time.Hour)),
			Compaction: tsdb.BlockMetaCompaction{
				Level: 1,
			},
		},
		Thanos: metadata.Thanos{
			Labels: lbls.Map(),
		},
	}

	// Upload block metadata using json.Marshal
	metaBytes, err := json.Marshal(meta)
	testutil.Ok(t, err)
	testutil.Ok(t, bkt.Upload(ctx, filepath.Join(blockID.String(), metadata.MetaFilename), bytes.NewReader(metaBytes)))

	// Upload a dummy index file
	testutil.Ok(t, bkt.Upload(ctx, filepath.Join(blockID.String(), block.IndexFilename), strings.NewReader("dummy-index")))

	// Upload a dummy chunk file
	testutil.Ok(t, bkt.Upload(ctx, filepath.Join(blockID.String(), "chunks", "000001"), strings.NewReader("dummy-chunk")))

	return blockID
}

func createDeletionMark(t *testing.T, ctx context.Context, bkt objstore.Bucket, blockID ulid.ULID) {
	deletionMark := metadata.DeletionMark{
		ID:           blockID,
		DeletionTime: time.Now().Unix(),
		Version:      metadata.DeletionMarkVersion1,
		Details:      "Test deletion mark",
	}

	deletionMarkBytes, err := json.Marshal(deletionMark)
	testutil.Ok(t, err)

	testutil.Ok(t, bkt.Upload(ctx, filepath.Join(blockID.String(), metadata.DeletionMarkFilename), bytes.NewReader(deletionMarkBytes)))
}

func verifyBlockExists(t *testing.T, ctx context.Context, bkt objstore.Bucket, blockID ulid.ULID, shouldExist bool) {
	exists, err := bkt.Exists(ctx, filepath.Join(blockID.String(), metadata.MetaFilename))
	testutil.Ok(t, err)

	if shouldExist {
		testutil.Assert(t, exists, "Block %s should exist in destination", blockID.String())
	} else {
		testutil.Assert(t, !exists, "Block %s should not exist in destination", blockID.String())
	}
}

func verifyDeletionMark(t *testing.T, ctx context.Context, bkt objstore.Bucket, blockID ulid.ULID, shouldExist bool) {
	exists, err := bkt.Exists(ctx, filepath.Join(blockID.String(), metadata.DeletionMarkFilename))
	testutil.Ok(t, err)

	if shouldExist {
		testutil.Assert(t, exists, "Deletion mark should exist for block %s", blockID.String())
	} else {
		testutil.Assert(t, !exists, "Deletion mark should not exist for block %s", blockID.String())
	}
}

func runMigrationCommand(t *testing.T, e e2e.Environment, sourceDir, destDir string, args ...string) {
	t.Helper()

	// Create config files for filesystem storage
	sourceConfig := fmt.Sprintf(`type: FILESYSTEM
config:
  directory: %s`, sourceDir)

	destConfig := fmt.Sprintf(`type: FILESYSTEM
config:
  directory: %s`, destDir)

	sourceConfigFile := filepath.Join(e.SharedDir(), "source-config.yaml")
	destConfigFile := filepath.Join(e.SharedDir(), "dest-config.yaml")

	testutil.Ok(t, os.WriteFile(sourceConfigFile, []byte(sourceConfig), 0644))
	testutil.Ok(t, os.WriteFile(destConfigFile, []byte(destConfig), 0644))

	// Build command arguments
	cmdArgs := []string{
		"bucket", "migrate",
		"--objstore.config-file=" + sourceConfigFile,
		"--objstore-to.config-file=" + destConfigFile,
	}
	cmdArgs = append(cmdArgs, args...)

	// Create a unique runnable name based on timestamp
	runnableName := fmt.Sprintf("thanos-migrate-%d", time.Now().UnixNano())

	// Create a long-running container that we can execute commands in
	migrate := e.Runnable(runnableName).Init(e2e.StartOptions{
		Image:   e2ethanos.DefaultImage(),
		Command: e2e.NewCommandWithoutEntrypoint("tail", "-f", "/dev/null"),
	})

	// Start the container and wait for it to be ready
	testutil.Ok(t, e2e.StartAndWaitReady(migrate))

	// Execute the migration command
	testutil.Ok(t, migrate.Exec(e2e.NewCommand("tools", cmdArgs...)))

	// Stop the container
	testutil.Ok(t, migrate.Stop())
}
