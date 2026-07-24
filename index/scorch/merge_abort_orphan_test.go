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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// abortMergePlugin wraps the default segment plugin. Once armed, the first
// file merge (all inputs persisted) succeeds and every later one fails,
// so a multi-task merge plan aborts after its first task has already
// written its output.
type abortMergePlugin struct {
	delegate SegmentPlugin
	armed    atomic.Bool
	calls    atomic.Int32

	mu        sync.Mutex
	firstPath string
}

func (p *abortMergePlugin) Type() string    { return "abortmerge" }
func (p *abortMergePlugin) Version() uint32 { return 1 }

func (p *abortMergePlugin) New(results []index.Document) (segment.Segment, uint64, error) {
	return p.delegate.New(results)
}

func (p *abortMergePlugin) NewUsing(results []index.Document, config map[string]interface{}) (segment.Segment, uint64, error) {
	return p.delegate.NewUsing(results, config)
}

func (p *abortMergePlugin) Open(path string) (segment.Segment, error) {
	return p.delegate.Open(path)
}

func (p *abortMergePlugin) OpenUsing(path string, config map[string]interface{}) (segment.Segment, error) {
	return p.delegate.OpenUsing(path, config)
}

// gate returns nil to let a merge proceed. Only armed merges whose inputs
// are all persisted count: the persister's in-memory flush merges must
// pass through untouched.
func (p *abortMergePlugin) gate(segments []segment.Segment, path string) error {
	if !p.armed.Load() {
		return nil
	}
	for _, s := range segments {
		if _, ok := s.(segment.PersistedSegment); !ok {
			return nil
		}
	}
	if p.calls.Add(1) == 1 {
		p.mu.Lock()
		p.firstPath = path
		p.mu.Unlock()
		return nil
	}
	return fmt.Errorf("injected merge failure")
}

func (p *abortMergePlugin) firstOutputPath() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstPath
}

func (p *abortMergePlugin) Merge(segments []segment.Segment, drops []*roaring.Bitmap, path string,
	closeCh chan struct{}, s segment.StatsReporter) ([][]uint64, uint64, error) {
	if err := p.gate(segments, path); err != nil {
		return nil, 0, err
	}
	return p.delegate.Merge(segments, drops, path, closeCh, s)
}

func (p *abortMergePlugin) MergeUsing(segments []segment.Segment, drops []*roaring.Bitmap, path string,
	closeCh chan struct{}, s segment.StatsReporter, config map[string]interface{}) ([][]uint64, uint64, error) {
	if err := p.gate(segments, path); err != nil {
		return nil, 0, err
	}
	return p.delegate.MergeUsing(segments, drops, path, closeCh, s, config)
}

var (
	abortMergeSetupOnce sync.Once
	abortMergeTestPlug  = &abortMergePlugin{}
	abortMergeAllow     atomic.Bool
)

// registered lazily so package init has already built the plugin registry
func abortMergeSetup() {
	abortMergeTestPlug.delegate = defaultSegmentPlugin
	RegisterSegmentPlugin(abortMergeTestPlug, false)
	RegistryEventCallbacks["mergeAbortOrphanTest"] = func(e Event) bool {
		if e.Kind == EventKindPreMergeCheck {
			return abortMergeAllow.Load()
		}
		return true
	}
}

// TestMergeAbortOrphanSweptWhilePersisterWaits pins the forced purge inside
// the persister's introducer-wait: a merge plan whose later task fails after
// an earlier task succeeded aborts before any introduction, leaving the
// succeeded task's output on disk, unmarked and unreferenced. No
// introduction ever arrives to wake the persister, so only a purge pass run
// while it waits can reclaim the file. Pre-patch the persister had no such
// pass and the orphan survived indefinitely (accumulating once per replan).
func TestMergeAbortOrphanSweptWhilePersisterWaits(t *testing.T) {
	abortMergeSetupOnce.Do(abortMergeSetup)
	abortMergeTestPlug.armed.Store(false)
	abortMergeTestPlug.calls.Store(0)
	abortMergeAllow.Store(false)

	origInterval := ForcedPurgeInterval
	ForcedPurgeInterval = 100 * time.Millisecond
	defer func() { ForcedPurgeInterval = origInterval }()

	cfg := CreateConfig("TestMergeAbortOrphanSweptWhilePersisterWaits")
	cfg["forceSegmentType"] = "abortmerge"
	cfg["forceSegmentVersion"] = 1
	cfg["eventCallbackName"] = "mergeAbortOrphanTest"
	// small tasks so one plan holds several
	cfg["scorchMergePlanOptions"] = map[string]interface{}{
		"maxSegmentsPerTier":   2,
		"segmentsPerMergeTask": 2,
	}
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
	s := idx.(*Scorch)

	// build up persisted, unmerged segments while file merges are vetoed
	for round := 0; round < 8; round++ {
		batch := index.NewBatch()
		for d := 0; d < 20; d++ {
			doc := document.NewDocument(fmt.Sprintf("b%d-d%d", round, d))
			doc.AddField(document.NewTextField("body", []uint64{},
				[]byte(fmt.Sprintf("segment %d body of doc %d", round, d))))
			batch.Update(doc)
		}
		if err := idx.Batch(batch); err != nil {
			t.Fatal(err)
		}
	}

	// wait for the persister to park in its introducer-wait, so the only
	// thing that can reclaim anything afterwards is a purge run from there
	parked := atomic.LoadUint64(&s.stats.TotPersistLoopWait)
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadUint64(&s.stats.TotPersistLoopWait) == parked {
		if time.Now().After(deadline) {
			t.Fatal("persister never parked after final batch")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// arm and release the merger: task 1 merges fine, task 2 fails, the
	// plan aborts with task 1's output orphaned on disk
	abortMergeTestPlug.armed.Store(true)
	abortMergeAllow.Store(true)

	deadline = time.Now().Add(10 * time.Second)
	for abortMergeTestPlug.calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("merge plan produced %d file-merge task(s), need >= 2; tune plan options",
				abortMergeTestPlug.calls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	orphan := abortMergeTestPlug.firstOutputPath()
	if orphan == "" {
		t.Fatal("no successful merge output recorded")
	}

	// the orphan must disappear within a bounded number of purge intervals,
	// with no successful merge introduction to do it as a side effect
	sweepDeadline := time.Now().Add(20 * ForcedPurgeInterval)
	for time.Now().Before(sweepDeadline) {
		if _, err := os.Stat(orphan); os.IsNotExist(err) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("aborted merge plan's output %s still on disk after %v",
			orphan, 20*ForcedPurgeInterval)
	}
	if n := atomic.LoadUint64(&s.stats.TotFileMergeIntroductions); n != 0 {
		t.Fatalf("expected zero merge introductions, got %d", n)
	}
}
