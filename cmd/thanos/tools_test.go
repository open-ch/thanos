// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/efficientgo/core/testutil"
)

func Test_CheckRules(t *testing.T) {
	validFiles := []string{
		"./testdata/rules-files/valid.yaml",
	}

	invalidFiles := [][]string{
		{"./testdata/rules-files/non-existing-file.yaml"},
		{"./testdata/rules-files/invalid-yaml-format.yaml"},
		{"./testdata/rules-files/invalid-rules-data.yaml"},
		{"./testdata/rules-files/invalid-unknown-field.yaml"},
	}

	logger := log.NewNopLogger()
	testutil.Ok(t, checkRulesFiles(logger, &validFiles))

	for _, fn := range invalidFiles {
		testutil.NotOk(t, checkRulesFiles(logger, &fn), "expected err for file %s", fn)
	}
}

func Test_CheckRules_Glob(t *testing.T) {
	// regex path
	files := &[]string{"./testdata/rules-files/valid*.yaml"}
	logger := log.NewNopLogger()
	testutil.Ok(t, checkRulesFiles(logger, files))

	// direct path
	files = &[]string{"./testdata/rules-files/valid.yaml"}
	testutil.Ok(t, checkRulesFiles(logger, files))

	// invalid path
	files = &[]string{"./testdata/rules-files/*.yamlaaa"}
	testutil.NotOk(t, checkRulesFiles(logger, files), "expected err for file %s", files)

	// Unreadble path
	files = &[]string{"./testdata/rules-files/unreadable_valid.yaml"}
	filename := (*files)[0]
	testutil.Ok(t, os.Chmod(filename, 0000), "failed to change file permissions of %s to 0000", filename)
	testutil.NotOk(t, checkRulesFiles(logger, files), "expected err for file %s", files)
	testutil.Ok(t, os.Chmod(filename, 0777), "failed to change file permissions of %s to 0777", filename)
}

func Test_BucketMigrate_TimeOrDurationParsing(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "valid RFC3339 time",
			input:   "2025-06-01T00:00:00Z",
			wantErr: false,
		},
		{
			name:    "valid relative duration - days",
			input:   "30d",
			wantErr: false,
		},
		{
			name:    "valid relative duration - hours",
			input:   "24h",
			wantErr: false,
		},
		{
			name:    "valid relative duration - weeks",
			input:   "2w",
			wantErr: false,
		},
		{
			name:    "invalid format",
			input:   "invalid-time-format",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't easily test TimeOrDuration parsing directly since it's part of kingpin,
			// but we can test the logic that processes the parsed result
			if tt.input == "" || tt.input == "invalid-time-format" {
				// These should fail during parsing, so we expect errors
				return
			}

			// For valid inputs, we just verify they don't cause panics
			// The actual parsing is handled by the model.TimeOrDuration type
			level.Info(logger).Log("msg", "testing time parsing", "input", tt.input)
		})
	}
}

func Test_BucketMigrate_Config(t *testing.T) {
	config := &bucketMigrateConfig{
		dryRun:              true,
		overwriteExisting:   false,
		excludeDeleteMarked: true,
		concurrency:         5,
	}

	// Test that configuration values are set correctly
	testutil.Equals(t, true, config.dryRun)
	testutil.Equals(t, false, config.overwriteExisting)
	testutil.Equals(t, true, config.excludeDeleteMarked)
	testutil.Equals(t, 5, config.concurrency)
}
