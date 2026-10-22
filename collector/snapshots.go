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
	"strings"
	"sync"

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

// snapshotStatusBatchSize bounds how many snapshot names go into one status URL.
const snapshotStatusBatchSize = 20

// Snapshots information struct
type Snapshots struct {
	logger *slog.Logger
	hc     *http.Client
	u      *url.URL

	// sizes caches finished snapshot sizes by repository and snapshot name. A finished
	// snapshot never changes size, and its status call reads metadata from the repository.
	mu    sync.Mutex
	sizes map[string]map[string]int64
}

// NewSnapshots defines Snapshots Prometheus metrics
func NewSnapshots(logger *slog.Logger, u *url.URL, hc *http.Client) (Collector, error) {
	return &Snapshots{
		logger: logger,
		u:      u,
		hc:     hc,
		sizes:  make(map[string]map[string]int64),
	}, nil
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

		sizes, err := c.snapshotSizes(ctx, repository, ssr.Snapshots)
		if err != nil {
			c.logger.Warn(
				"failed to fetch snapshot status",
				"repository", repository,
				"err", err,
			)
		}
		snapshotSizes[repository] = sizes
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

// snapshotSizes returns the size of every listed snapshot in the repository. Sizes of
// finished snapshots come from the cache; the rest are read from the status API by name,
// because /_snapshot/{repo}/_all/_status only reports snapshots that are still running.
// On a status error it returns the sizes it has, so cached series keep reporting.
func (c *Snapshots) snapshotSizes(ctx context.Context, repository string, snapshots []SnapshotStatDataResponse) (map[string]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cached := c.sizes[repository]
	sizes := make(map[string]int64, len(snapshots))
	finished := make(map[string]int64, len(snapshots))
	var missing []string
	for _, snap := range snapshots {
		if size, ok := cached[snap.Snapshot]; ok {
			sizes[snap.Snapshot] = size
			finished[snap.Snapshot] = size
			continue
		}
		missing = append(missing, snap.Snapshot)
	}

	var fetchErr error
	for start := 0; start < len(missing); start += snapshotStatusBatchSize {
		end := min(start+snapshotStatusBatchSize, len(missing))
		status, err := c.snapshotStatus(ctx, repository, missing[start:end])
		if err != nil {
			fetchErr = err
			break
		}
		for _, snap := range status.Snapshots {
			sizes[snap.Snapshot] = snap.Stats.Total.SizeInBytes
			if snap.State != "IN_PROGRESS" && snap.State != "STARTED" {
				finished[snap.Snapshot] = snap.Stats.Total.SizeInBytes
			}
		}
	}

	c.sizes[repository] = finished
	return sizes, fetchErr
}

// snapshotStatus reads the stored size of the named snapshots from the snapshot status API.
// filter_path limits the response to name, state and total size. Elasticsearch still
// reads shard metadata in the repository to build that response.
func (c *Snapshots) snapshotStatus(ctx context.Context, repository string, names []string) (SnapshotStatusResponse, error) {
	var status SnapshotStatusResponse
	u := c.u.ResolveReference(&url.URL{
		Path: path.Join("/_snapshot", repository, strings.Join(names, ","), "_status"),
		RawQuery: url.Values{
			"filter_path": []string{"snapshots.snapshot,snapshots.state,snapshots.stats.total.size_in_bytes"},
		}.Encode(),
	})

	resp, err := getURL(ctx, c.hc, c.logger, u.String())
	if err != nil {
		return status, err
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return status, fmt.Errorf("failed to unmarshal snapshot status JSON: %v", err)
	}
	return status, nil
}
