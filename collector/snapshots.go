// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	defaultSnapshotLabels      = []string{"repository", "state", "version"}
	defaultSnapshotLabelValues = func(repositoryName string, snapshotStats SnapshotStatDataResponse) []string {
		return []string{repositoryName, snapshotStats.State, snapshotStats.Version}
	}
	defaultSnapshotRepositoryLabels      = []string{"repository"}
	defaultSnapshotRepositoryLabelValues = func(repositoryName string) []string {
		return []string{repositoryName}
	}
)

var (
	numIndices = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_number_of_indices"),
		"Number of indices in the last snapshot",
		defaultSnapshotLabels, nil,
	)
	snapshotStartTimestamp = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_start_time_timestamp"),
		"Last snapshot start timestamp",
		defaultSnapshotLabels, nil,
	)
	snapshotEndTimestamp = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_end_time_timestamp"),
		"Last snapshot end timestamp",
		defaultSnapshotLabels, nil,
	)
	snapshotNumFailures = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_number_of_failures"),
		"Last snapshot number of failures",
		defaultSnapshotLabels, nil,
	)
	snapshotNumShards = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_total_shards"),
		"Last snapshot total shards",
		defaultSnapshotLabels, nil,
	)
	snapshotFailedShards = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_failed_shards"),
		"Last snapshot failed shards",
		defaultSnapshotLabels, nil,
	)
	snapshotSuccessfulShards = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_successful_shards"),
		"Last snapshot successful shards",
		defaultSnapshotLabels, nil,
	)

	numSnapshots = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "number_of_snapshots"),
		"Number of snapshots in a repository",
		defaultSnapshotRepositoryLabels, nil,
	)
	oldestSnapshotTimestamp = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "oldest_snapshot_timestamp"),
		"Timestamp of the oldest snapshot",
		defaultSnapshotRepositoryLabels, nil,
	)
	latestSnapshotTimestamp = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "latest_snapshot_timestamp_seconds"),
		"Timestamp of the latest SUCCESS or PARTIAL snapshot",
		defaultSnapshotRepositoryLabels, nil,
	)
	snapshotSizeBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "snapshot_stats", "snapshot_size_bytes"),
		"Total size of each snapshot in bytes",
		[]string{"repository", "snapshot"}, nil,
	)
)

func init() {
	registerCollector("snapshots", defaultDisabled, NewSnapshots)
}

const (
	// One snapshot per status call. Elasticsearch reads that snapshot's repository
	// metadata to answer, and a single large snapshot can take longer than the
	// scrape timeout. Batching several into one URL fails the whole request.
	snapshotStatusTimeout = 2 * time.Minute
	snapshotStatusRetry   = time.Minute
	snapshotStatusIdle    = 15 * time.Second
)

// snapshotStatusAsync fetches sizes outside the Prometheus scrape. Tests turn it
// off so one Collect still reads every snapshot from the fake server.
var snapshotStatusAsync = true

// Snapshots information struct
type Snapshots struct {
	logger *slog.Logger
	hc     *http.Client
	u      *url.URL

	statusHC *http.Client

	mu       sync.Mutex
	sizes    map[string]map[string]int64
	terminal map[string]map[string]struct{}
	wanted   map[string][]SnapshotStatDataResponse
	retryAt  map[string]time.Time

	wake chan struct{}
	done chan struct{}
	stop sync.Once
}

// NewSnapshots defines Snapshots Prometheus metrics
func NewSnapshots(logger *slog.Logger, u *url.URL, hc *http.Client) (Collector, error) {
	c := &Snapshots{
		logger:   logger,
		u:        u,
		hc:       hc,
		sizes:    make(map[string]map[string]int64),
		terminal: make(map[string]map[string]struct{}),
		wanted:   make(map[string][]SnapshotStatDataResponse),
		retryAt:  make(map[string]time.Time),
	}
	if snapshotStatusAsync {
		cloned := *hc
		if cloned.Timeout < snapshotStatusTimeout {
			cloned.Timeout = snapshotStatusTimeout
		}
		c.statusHC = &cloned
		c.wake = make(chan struct{}, 1)
		c.done = make(chan struct{})
		go c.loop()
	}
	return c, nil
}

func (c *Snapshots) stopStatusFetch() {
	if c.done == nil {
		return
	}
	c.stop.Do(func() { close(c.done) })
}

func (c *Snapshots) Update(ctx context.Context, ch chan<- prometheus.Metric) error {
	// indices
	snapshotsStatsResp := make(map[string]SnapshotStatsResponse)
	snapshotSizes := make(map[string]map[string]int64)
	u := c.u.ResolveReference(&url.URL{Path: "/_snapshot"})

	var srr SnapshotRepositoriesResponse
	resp, err := getURL(ctx, c.hc, c.logger, u.String())
	if err != nil {
		return err
	}

	err = json.Unmarshal(resp, &srr)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	for repository := range srr {
		pathPart := path.Join("/_snapshot", repository, "/_all")
		u := c.u.ResolveReference(&url.URL{Path: pathPart})
		var ssr SnapshotStatsResponse
		resp, err := getURL(ctx, c.hc, c.logger, u.String())
		if err != nil {
			continue
		}
		err = json.Unmarshal(resp, &ssr)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %v", err)
		}
		snapshotsStatsResp[repository] = ssr

		if snapshotStatusAsync {
			snapshotSizes[repository] = c.remember(repository, ssr.Snapshots)
			continue
		}
		sizes, _ := c.snapshotSizes(ctx, repository, ssr.Snapshots)
		snapshotSizes[repository] = sizes
	}
	if snapshotStatusAsync {
		c.pruneRepos(snapshotsStatsResp)
		c.kick()
	}

	// Snapshots stats
	for repositoryName, snapshotStats := range snapshotsStatsResp {

		ch <- prometheus.MustNewConstMetric(
			numSnapshots,
			prometheus.GaugeValue,
			float64(len(snapshotStats.Snapshots)),
			defaultSnapshotRepositoryLabelValues(repositoryName)...,
		)

		oldest := float64(0)
		if len(snapshotStats.Snapshots) > 0 {
			oldest = float64(snapshotStats.Snapshots[0].StartTimeInMillis / 1000)
		}
		ch <- prometheus.MustNewConstMetric(
			oldestSnapshotTimestamp,
			prometheus.GaugeValue,
			oldest,
			defaultSnapshotRepositoryLabelValues(repositoryName)...,
		)

		latest := float64(0)
		for i := len(snapshotStats.Snapshots) - 1; i >= 0; i-- {
			var snap = snapshotStats.Snapshots[i]
			if snap.State == "SUCCESS" || snap.State == "PARTIAL" {
				latest = float64(snap.StartTimeInMillis / 1000)
				break
			}
		}
		ch <- prometheus.MustNewConstMetric(
			latestSnapshotTimestamp,
			prometheus.GaugeValue,
			latest,
			defaultSnapshotRepositoryLabelValues(repositoryName)...,
		)

		for name, size := range snapshotSizes[repositoryName] {
			ch <- prometheus.MustNewConstMetric(
				snapshotSizeBytes,
				prometheus.GaugeValue,
				float64(size),
				repositoryName, name,
			)
		}

		if len(snapshotStats.Snapshots) == 0 {
			continue
		}

		lastSnapshot := snapshotStats.Snapshots[len(snapshotStats.Snapshots)-1]
		ch <- prometheus.MustNewConstMetric(
			numIndices,
			prometheus.GaugeValue,
			float64(len(lastSnapshot.Indices)),
			defaultSnapshotLabelValues(repositoryName, lastSnapshot)...,
		)
		ch <- prometheus.MustNewConstMetric(
			snapshotStartTimestamp,
			prometheus.GaugeValue,
			float64(lastSnapshot.StartTimeInMillis/1000),
			defaultSnapshotLabelValues(repositoryName, lastSnapshot)...,
		)
		ch <- prometheus.MustNewConstMetric(
			snapshotEndTimestamp,
			prometheus.GaugeValue,
			float64(lastSnapshot.EndTimeInMillis/1000),
			defaultSnapshotLabelValues(repositoryName, lastSnapshot)...,
		)
		ch <- prometheus.MustNewConstMetric(
			snapshotNumFailures,
			prometheus.GaugeValue,
			float64(len(lastSnapshot.Failures)),
			defaultSnapshotLabelValues(repositoryName, lastSnapshot)...,
		)
		ch <- prometheus.MustNewConstMetric(
			snapshotNumShards,
			prometheus.GaugeValue,
			float64(lastSnapshot.Shards.Total),
			defaultSnapshotLabelValues(repositoryName, lastSnapshot)...,
		)
		ch <- prometheus.MustNewConstMetric(
			snapshotFailedShards,
			prometheus.GaugeValue,
			float64(lastSnapshot.Shards.Failed),
			defaultSnapshotLabelValues(repositoryName, lastSnapshot)...,
		)
		ch <- prometheus.MustNewConstMetric(
			snapshotSuccessfulShards,
			prometheus.GaugeValue,
			float64(lastSnapshot.Shards.Successful),
			defaultSnapshotLabelValues(repositoryName, lastSnapshot)...,
		)
	}

	return nil
}

// snapshotSizes reads each missing snapshot on its own, newest first. Used when
// snapshotStatusAsync is false. A failed snapshot is skipped so the next one still runs.
func (c *Snapshots) snapshotSizes(ctx context.Context, repository string, snapshots []SnapshotStatDataResponse) (map[string]int64, error) {
	c.mu.Lock()
	cached := c.sizes[repository]
	sizes := make(map[string]int64, len(snapshots))
	finished := make(map[string]int64, len(snapshots))
	var missing []SnapshotStatDataResponse
	for _, snap := range snapshots {
		if size, ok := cached[snap.Snapshot]; ok {
			sizes[snap.Snapshot] = size
			finished[snap.Snapshot] = size
			continue
		}
		missing = append(missing, snap)
	}
	c.mu.Unlock()

	sort.SliceStable(missing, func(i, j int) bool {
		return missing[i].StartTimeInMillis > missing[j].StartTimeInMillis
	})

	var fetchErr error
	for _, snap := range missing {
		status, err := c.snapshotStatus(ctx, c.hc, repository, snap.Snapshot)
		if err != nil {
			fetchErr = err
			c.logger.Warn(
				"failed to fetch snapshot status",
				"repository", repository,
				"snapshot", snap.Snapshot,
				"err", err,
			)
			continue
		}
		for _, got := range status.Snapshots {
			if got.Snapshot != snap.Snapshot {
				continue
			}
			sizes[got.Snapshot] = got.Stats.Total.SizeInBytes
			if snapshotStateTerminal(got.State) {
				finished[got.Snapshot] = got.Stats.Total.SizeInBytes
			}
		}
	}

	c.mu.Lock()
	c.sizes[repository] = finished
	c.mu.Unlock()
	return sizes, fetchErr
}

func snapshotStateTerminal(state string) bool {
	switch state {
	case "SUCCESS", "PARTIAL", "FAILED", "ABORTED":
		return true
	default:
		return false
	}
}

func snapKey(repository, name string) string {
	return repository + "\x00" + name
}

// remember records the snapshots a scrape just listed and returns sizes already known.
// Names that disappeared are dropped, so a deleted snapshot is not emitted again.
func (c *Snapshots) remember(repository string, snapshots []SnapshotStatDataResponse) map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.wanted[repository] = snapshots
	listed := make(map[string]struct{}, len(snapshots))
	for _, snap := range snapshots {
		listed[snap.Snapshot] = struct{}{}
	}
	for name := range c.sizes[repository] {
		if _, ok := listed[name]; ok {
			continue
		}
		delete(c.sizes[repository], name)
		delete(c.terminal[repository], name)
		delete(c.retryAt, snapKey(repository, name))
	}
	out := make(map[string]int64, len(c.sizes[repository]))
	for name, size := range c.sizes[repository] {
		out[name] = size
	}
	return out
}

func (c *Snapshots) pruneRepos(seen map[string]SnapshotStatsResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for repo := range c.wanted {
		if _, ok := seen[repo]; ok {
			continue
		}
		delete(c.wanted, repo)
		delete(c.sizes, repo)
		delete(c.terminal, repo)
		prefix := repo + "\x00"
		for key := range c.retryAt {
			if strings.HasPrefix(key, prefix) {
				delete(c.retryAt, key)
			}
		}
	}
}

func (c *Snapshots) kick() {
	if c.wake == nil {
		return
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Snapshots) loop() {
	timer := time.NewTimer(snapshotStatusIdle)
	defer timer.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-c.wake:
		case <-timer.C:
		}
		for c.fetchNext() {
			select {
			case <-c.done:
				return
			default:
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(snapshotStatusIdle)
	}
}

// fetchNext reads one uncached snapshot. It returns false when nothing is due.
func (c *Snapshots) fetchNext() bool {
	repo, name, ok := c.pick()
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotStatusTimeout)
	defer cancel()
	status, err := c.snapshotStatus(ctx, c.statusHC, repo, name)
	c.record(repo, name, status, err)
	return true
}

// pick chooses the newest listed snapshot that is not cached as terminal and is not
// waiting out a failure. The choice is reserved so the next call does not start it too.
func (c *Snapshots) pick() (string, string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var repo, name string
	var start int64
	found := false
	for r, snaps := range c.wanted {
		for _, snap := range snaps {
			if _, done := c.terminal[r][snap.Snapshot]; done {
				continue
			}
			if until, waiting := c.retryAt[snapKey(r, snap.Snapshot)]; waiting && until.After(now) {
				continue
			}
			if found && (snap.StartTimeInMillis < start || (snap.StartTimeInMillis == start && snap.Snapshot <= name)) {
				continue
			}
			found = true
			repo = r
			name = snap.Snapshot
			start = snap.StartTimeInMillis
		}
	}
	if !found {
		return "", "", false
	}
	c.retryAt[snapKey(repo, name)] = now.Add(snapshotStatusTimeout)
	return repo, name, true
}

func (c *Snapshots) record(repository, name string, status SnapshotStatusResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := snapKey(repository, name)
	if !c.wantedName(repository, name) {
		delete(c.retryAt, key)
		return
	}
	if err != nil {
		c.logger.Warn(
			"failed to fetch snapshot status",
			"repository", repository,
			"snapshot", name,
			"err", err,
		)
		c.retryAt[key] = time.Now().Add(snapshotStatusRetry)
		return
	}
	for _, snap := range status.Snapshots {
		if snap.Snapshot != name {
			continue
		}
		if c.sizes[repository] == nil {
			c.sizes[repository] = map[string]int64{}
		}
		c.sizes[repository][name] = snap.Stats.Total.SizeInBytes
		if snapshotStateTerminal(snap.State) {
			if c.terminal[repository] == nil {
				c.terminal[repository] = map[string]struct{}{}
			}
			c.terminal[repository][name] = struct{}{}
			delete(c.retryAt, key)
			return
		}
		c.retryAt[key] = time.Now().Add(snapshotStatusRetry)
		return
	}
	c.retryAt[key] = time.Now().Add(snapshotStatusRetry)
}

func (c *Snapshots) wantedName(repository, name string) bool {
	for _, snap := range c.wanted[repository] {
		if snap.Snapshot == name {
			return true
		}
	}
	return false
}

// snapshotStatus reads one snapshot's stored size. filter_path limits the response
// to name, state and total size. Elasticsearch still reads that snapshot's shard
// metadata in the repository to build the response.
func (c *Snapshots) snapshotStatus(ctx context.Context, hc *http.Client, repository, name string) (SnapshotStatusResponse, error) {
	var status SnapshotStatusResponse
	u := c.u.ResolveReference(&url.URL{
		Path: path.Join("/_snapshot", repository, name, "_status"),
		RawQuery: url.Values{
			"filter_path": []string{"snapshots.snapshot,snapshots.state,snapshots.stats.total.size_in_bytes"},
		}.Encode(),
	})

	resp, err := getURL(ctx, hc, c.logger, u.String())
	if err != nil {
		return status, err
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return status, fmt.Errorf("failed to unmarshal snapshot status JSON: %v", err)
	}
	return status, nil
}
