//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package scorch

import (
	"testing"
	"time"
)

// This file is the compile fence for the forced-purge knob: it references
// ForcedPurgeInterval directly, so it only builds on a scorch that has it,
// and it wires the shared hook the behavioral test uses (that test file
// deliberately compiles without the knob).
func init() {
	shortenForcedPurgeInterval = func(d time.Duration) func() {
		orig := ForcedPurgeInterval
		ForcedPurgeInterval = d
		return func() { ForcedPurgeInterval = orig }
	}
}

// TestForcedPurgeIntervalDefault pins that the forced purge ships enabled;
// a zero default silently reverts to idle-only cleanup.
func TestForcedPurgeIntervalDefault(t *testing.T) {
	if ForcedPurgeInterval <= 0 {
		t.Fatalf("ForcedPurgeInterval default must be > 0, got %v", ForcedPurgeInterval)
	}
}
