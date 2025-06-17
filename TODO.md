# Thanos Bucket Migration Tool - Implementation Tasks

## Phase 1: Command Structure and Basic Setup
- [x] **Add new migrate command to CLI structure**
  Add `migrate` subcommand to `thanos bucket tools` in `cmd/thanos/tools_bucket.go`
  Use the existing `replicate` command as a template for basic structure
  Set up command registration and basic help text

## Phase 2: CLI Arguments and Configuration
- [x] **Implement cutoff date parsing**
  Add `--older-than` flag supporting both RFC3339 absolute times and relative formats (e.g., "30d", "2025-06-01")
  Create utility functions to parse and convert relative times to absolute timestamps
  Add validation for date format inputs

- [x] **Add block handling configuration flags**
  Add `--overwrite-existing` flag to control duplicate block behavior
  Add `--dry-run` flag for preview mode
  Add `--exclude-delete-marked` flag to skip blocks already marked for deletion

- [x] **Add operational control flags**
  Add `--concurrency` flag for parallel migration control
  Add `--selector.relabel-config` flag (reuse existing implementation from bucket web command)

## Phase 3: Core Migration Logic
- [x] **Implement block discovery and filtering**
  Create function to list all blocks from source storage using existing storage client
  Implement filtering logic to exclude blocks marked for deletion (if flag set)
  Add block metadata parsing to extract TSDB min time for age comparison

- [x] **Add cutoff date filtering**
  Implement logic to compare block min time against cutoff date
  Apply selector relabel config filtering to blocks
  Create summary logging of blocks selected for migration

- [x] **Implement dry-run functionality**
  Add dry-run mode that lists blocks that would be migrated without performing actual migration
  Include block details (ID, size, min time) in dry-run output
  Ensure dry-run respects all filtering options

## Phase 4: Migration and Replication
- [x] **Implement concurrent block migration**
  Create worker pool for concurrent block processing using configurable concurrency
  Reuse existing replication logic from replicate command for actual block copying
  Handle destination storage configuration and connection

- [x] **Add block copying and deletion marking**
  Implement block file copying from source to destination storage
  Add deletion mark creation in source storage after successful migration
  Handle existing blocks in destination (skip or overwrite based on flag)

- [x] **Fix deletion mark filtering**
  Ensure deletion-marked blocks are properly excluded during discovery phase
  Use proper concurrency parameters in NewIgnoreDeletionMarkFilter
  Verify that blocks with deletion marks don't appear in migration candidates

- [x] **Add duplicate block handling and counting**
  Implement proper handling of blocks that already exist in destination
  Create separate counters for migrated vs skipped blocks
  Return special error for existing blocks to distinguish from failures
  Log decisions for each block (migrated/skipped/overwritten)

## Phase 5: Deletion and Cleanup
- [x] **Implement source block deletion marking**
  Add logic to mark source blocks for deletion only after successful replication
  Ensure deletion marking only occurs when migration completes successfully
  Add appropriate logging for deletion marking activities

- [x] **Add comprehensive error handling**
  Implement proper error handling for all migration steps
  Ensure failed migrations don't prevent processing of other blocks
  Log all errors with appropriate context and block identifiers

## Phase 6: Integration and Polish
- [x] **Integrate logging and tracing**
  Use existing Thanos logging patterns throughout the migration process
  Add structured logging for migration progress, successes, and failures
  Integrate with existing tracing infrastructure

- [x] **Add comprehensive testing**
  Create unit tests for date parsing utilities
  Add integration tests for the migration workflow
  Test dry-run functionality and all configuration combinations

- [x] **Documentation and help text**
  Add comprehensive help text and examples for the migrate command
  Document all CLI flags and their usage
  Add usage examples for common scenarios (hot-to-cold migration)

## Phase 7: Validation and Finalization
- [x] **End-to-end testing**
  Test full migration workflow with real storage backends
  Verify idempotent behavior across multiple runs
  Test error scenarios and recovery

- [x] **Performance optimization**
  Optimize memory usage for large block lists
  Ensure efficient concurrent operations
  Add progress reporting for long-running operations

---

**Note:** Each task should be implemented as a separate commit to maintain clear development history and enable easy review of individual components.
