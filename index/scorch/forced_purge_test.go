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
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

// shortenForcedPurgeInterval shrinks the forced-purge interval for the test
// window and returns a restore func. It is assigned by an init() in
// forced_purge_fence_test.go so that this file compiles against a scorch
// without the forced-purge knob; such builds leave it nil and exhibit
// exactly the starvation this test pins.
var shortenForcedPurgeInterval func(d time.Duration) (restore func())

// TestForcedPurgeUnderSustainedMutation pins the forced purge: obsolete
// segment files must be reclaimed WHILE the index is under continuous
// mutation, before any quiescence. The batches are unsafe (no wait for
// persist) and issued back-to-back from a writer goroutine, so the persister
// stays behind the root epoch and never reaches its idle-wait purge; the
// .zap census is sampled mid-churn, not after the writer stops. Without the
// forced purge nothing prunes old bolt snapshots either, so every persisted
// epoch pins its segment files forever and the mid-churn census climbs
// without bound (idle-only cleanup was the pre-patch behavior and fails
// this test).
func TestForcedPurgeUnderSustainedMutation(t *testing.T) {
	if shortenForcedPurgeInterval != nil {
		restore := shortenForcedPurgeInterval(20 * time.Millisecond)
		defer restore()
	}

	cfg := CreateConfig("TestForcedPurgeUnderSustainedMutation")
	cfg["unsafe_batch"] = true // writer must outrun the persister or it idles
	if err := InitTest(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := DestroyTest(cfg); err != nil {
			t.Log(err)
		}
	}()
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := idx.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	countZap := func() int {
		entries, err := os.ReadDir(cfg["path"].(string))
		if err != nil {
			return 0 // dir mid-rename; sample again next tick
		}
		n := 0
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".zap") {
				n++
			}
		}
		return n
	}

	// Writer: update the same small doc set as fast as batches are accepted,
	// so every batch obsoletes prior segments and the root epoch stays ahead
	// of the persister for the whole window.
	var stop atomic.Bool
	var writerErr atomic.Value
	done := make(chan struct{})
	go func() {
		defer close(done)
		for round := 0; !stop.Load(); round++ {
			batch := index.NewBatch()
			for d := 0; d < 10; d++ {
				doc := document.NewDocument(fmt.Sprintf("doc-%d", d))
				doc.AddField(document.NewTextField("body", []uint64{},
					[]byte(fmt.Sprintf("round %d body of doc %d", round, d))))
				batch.Update(doc)
			}
			if err := idx.Batch(batch); err != nil {
				writerErr.Store(err)
				return
			}
		}
	}()

	// Sample the census mid-churn for ~2s. The assertion is on the maximum
	// observed while the writer is still running.
	maxZap := 0
	for i := 0; i < 80; i++ {
		time.Sleep(25 * time.Millisecond)
		if n := countZap(); n > maxZap {
			maxZap = n
		}
	}
	stop.Store(true)
	<-done
	if err, _ := writerErr.Load().(error); err != nil {
		t.Fatal(err)
	}

	// With the 20ms forced purge, stale bolt snapshots are pruned and their
	// files swept continuously, so the census stays near the live segment
	// set. Idle-only cleanup (ForcedPurgeInterval = 0) lets every persisted
	// epoch pin its files and the mid-churn maximum climbs far past this.
	t.Logf("mid-churn max .zap census: %d", maxZap)
	if maxZap > 150 {
		t.Fatalf("forced purge did not reclaim obsolete segments mid-churn: max %d .zap files on disk", maxZap)
	}
}
