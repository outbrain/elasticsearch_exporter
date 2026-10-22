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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/promslog"
)

func TestMain(m *testing.M) {
	snapshotStatusAsync = false
	os.Exit(m.Run())
}

func TestSnapshots(t *testing.T) {
	// Testcases created using:
	//  docker run -d -p 9200:9200 elasticsearch:VERSION-alpine  -Des.path.repo="/tmp" (1.7.6, 2.4.5)
	//  docker run -d -p 9200:9200 elasticsearch:VERSION-alpine  -E path.repo="/tmp" (5.4.2)
	//  curl -XPUT http://localhost:9200/foo_1/type1/1 -d '{"title":"abc","content":"hello"}'
	//  curl -XPUT http://localhost:9200/foo_1/type1/2 -d '{"title":"def","content":"world"}'
	//  curl -XPUT http://localhost:9200/foo_2/type1/1 -d '{"title":"abc001","content":"hello001"}'
	//  curl -XPUT http://localhost:9200/foo_2/type1/2 -d '{"title":"def002","content":"world002"}'
	//  curl -XPUT http://localhost:9200/foo_2/type1/3 -d '{"title":"def003","content":"world003"}'
	//  curl -XPUT http://localhost:9200/_snapshot/test1 -d '{"type": "fs","settings":{"location": "/tmp/test1"}}'
	//  curl -XPUT "http://localhost:9200/_snapshot/test1/snapshot_1?wait_for_completion=true"
	//  curl http://localhost:9200/_snapshot/
	//  curl http://localhost:9200/_snapshot/test1/_all

	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "1.7.6",
			file: "../fixtures/snapshots/1.7.6.json",
			want: `# HELP elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds Timestamp of the latest SUCCESS or PARTIAL snapshot
						# TYPE elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds gauge
						elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds{repository="test1"} 1.536052142e+09
						# HELP elasticsearch_snapshot_stats_number_of_snapshots Number of snapshots in a repository
						# TYPE elasticsearch_snapshot_stats_number_of_snapshots gauge
						elasticsearch_snapshot_stats_number_of_snapshots{repository="test1"} 1
						# HELP elasticsearch_snapshot_stats_oldest_snapshot_timestamp Timestamp of the oldest snapshot
						# TYPE elasticsearch_snapshot_stats_oldest_snapshot_timestamp gauge
						elasticsearch_snapshot_stats_oldest_snapshot_timestamp{repository="test1"} 1.536052142e+09
						# HELP elasticsearch_snapshot_stats_snapshot_end_time_timestamp Last snapshot end timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_end_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_end_time_timestamp{repository="test1",state="SUCCESS",version="1.7.6"} 1.536052142e+09
						# HELP elasticsearch_snapshot_stats_snapshot_failed_shards Last snapshot failed shards
						# TYPE elasticsearch_snapshot_stats_snapshot_failed_shards gauge
						elasticsearch_snapshot_stats_snapshot_failed_shards{repository="test1",state="SUCCESS",version="1.7.6"} 0
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_failures Last snapshot number of failures
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_failures gauge
						elasticsearch_snapshot_stats_snapshot_number_of_failures{repository="test1",state="SUCCESS",version="1.7.6"} 0
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_indices Number of indices in the last snapshot
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_indices gauge
						elasticsearch_snapshot_stats_snapshot_number_of_indices{repository="test1",state="SUCCESS",version="1.7.6"} 2
						# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes
						# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge
						elasticsearch_snapshot_stats_snapshot_size_bytes{repository="test1",snapshot="snapshot_1"} 6024
						# HELP elasticsearch_snapshot_stats_snapshot_start_time_timestamp Last snapshot start timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_start_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_start_time_timestamp{repository="test1",state="SUCCESS",version="1.7.6"} 1.536052142e+09
						# HELP elasticsearch_snapshot_stats_snapshot_successful_shards Last snapshot successful shards
						# TYPE elasticsearch_snapshot_stats_snapshot_successful_shards gauge
						elasticsearch_snapshot_stats_snapshot_successful_shards{repository="test1",state="SUCCESS",version="1.7.6"} 10
						# HELP elasticsearch_snapshot_stats_snapshot_total_shards Last snapshot total shards
						# TYPE elasticsearch_snapshot_stats_snapshot_total_shards gauge
						elasticsearch_snapshot_stats_snapshot_total_shards{repository="test1",state="SUCCESS",version="1.7.6"} 10
`,
		},
		{
			name: "2.4.5",
			file: "../fixtures/snapshots/2.4.5.json",
			want: `# HELP elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds Timestamp of the latest SUCCESS or PARTIAL snapshot
						# TYPE elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds gauge
						elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds{repository="test1"} 1.536053125e+09
						# HELP elasticsearch_snapshot_stats_number_of_snapshots Number of snapshots in a repository
						# TYPE elasticsearch_snapshot_stats_number_of_snapshots gauge
						elasticsearch_snapshot_stats_number_of_snapshots{repository="test1"} 1
						# HELP elasticsearch_snapshot_stats_oldest_snapshot_timestamp Timestamp of the oldest snapshot
						# TYPE elasticsearch_snapshot_stats_oldest_snapshot_timestamp gauge
						elasticsearch_snapshot_stats_oldest_snapshot_timestamp{repository="test1"} 1.536053125e+09
						# HELP elasticsearch_snapshot_stats_snapshot_end_time_timestamp Last snapshot end timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_end_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_end_time_timestamp{repository="test1",state="SUCCESS",version="2.4.5"} 1.536053126e+09
						# HELP elasticsearch_snapshot_stats_snapshot_failed_shards Last snapshot failed shards
						# TYPE elasticsearch_snapshot_stats_snapshot_failed_shards gauge
						elasticsearch_snapshot_stats_snapshot_failed_shards{repository="test1",state="SUCCESS",version="2.4.5"} 0
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_failures Last snapshot number of failures
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_failures gauge
						elasticsearch_snapshot_stats_snapshot_number_of_failures{repository="test1",state="SUCCESS",version="2.4.5"} 0
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_indices Number of indices in the last snapshot
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_indices gauge
						elasticsearch_snapshot_stats_snapshot_number_of_indices{repository="test1",state="SUCCESS",version="2.4.5"} 2
						# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes
						# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge
						elasticsearch_snapshot_stats_snapshot_size_bytes{repository="test1",snapshot="snapshot_1"} 6024
						# HELP elasticsearch_snapshot_stats_snapshot_start_time_timestamp Last snapshot start timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_start_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_start_time_timestamp{repository="test1",state="SUCCESS",version="2.4.5"} 1.536053125e+09
						# HELP elasticsearch_snapshot_stats_snapshot_successful_shards Last snapshot successful shards
						# TYPE elasticsearch_snapshot_stats_snapshot_successful_shards gauge
						elasticsearch_snapshot_stats_snapshot_successful_shards{repository="test1",state="SUCCESS",version="2.4.5"} 10
						# HELP elasticsearch_snapshot_stats_snapshot_total_shards Last snapshot total shards
						# TYPE elasticsearch_snapshot_stats_snapshot_total_shards gauge
						elasticsearch_snapshot_stats_snapshot_total_shards{repository="test1",state="SUCCESS",version="2.4.5"} 10
						`,
		},
		{
			name: "5.4.2",
			file: "../fixtures/snapshots/5.4.2.json",
			want: `# HELP elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds Timestamp of the latest SUCCESS or PARTIAL snapshot
						# TYPE elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds gauge
						elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds{repository="test1"} 1.536053353e+09
						# HELP elasticsearch_snapshot_stats_number_of_snapshots Number of snapshots in a repository
						# TYPE elasticsearch_snapshot_stats_number_of_snapshots gauge
						elasticsearch_snapshot_stats_number_of_snapshots{repository="test1"} 1
						# HELP elasticsearch_snapshot_stats_oldest_snapshot_timestamp Timestamp of the oldest snapshot
						# TYPE elasticsearch_snapshot_stats_oldest_snapshot_timestamp gauge
						elasticsearch_snapshot_stats_oldest_snapshot_timestamp{repository="test1"} 1.536053353e+09
						# HELP elasticsearch_snapshot_stats_snapshot_end_time_timestamp Last snapshot end timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_end_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_end_time_timestamp{repository="test1",state="SUCCESS",version="5.4.2"} 1.536053354e+09
						# HELP elasticsearch_snapshot_stats_snapshot_failed_shards Last snapshot failed shards
						# TYPE elasticsearch_snapshot_stats_snapshot_failed_shards gauge
						elasticsearch_snapshot_stats_snapshot_failed_shards{repository="test1",state="SUCCESS",version="5.4.2"} 0
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_failures Last snapshot number of failures
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_failures gauge
						elasticsearch_snapshot_stats_snapshot_number_of_failures{repository="test1",state="SUCCESS",version="5.4.2"} 0
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_indices Number of indices in the last snapshot
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_indices gauge
						elasticsearch_snapshot_stats_snapshot_number_of_indices{repository="test1",state="SUCCESS",version="5.4.2"} 2
						# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes
						# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge
						elasticsearch_snapshot_stats_snapshot_size_bytes{repository="test1",snapshot="snapshot_1"} 6024
						# HELP elasticsearch_snapshot_stats_snapshot_start_time_timestamp Last snapshot start timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_start_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_start_time_timestamp{repository="test1",state="SUCCESS",version="5.4.2"} 1.536053353e+09
						# HELP elasticsearch_snapshot_stats_snapshot_successful_shards Last snapshot successful shards
						# TYPE elasticsearch_snapshot_stats_snapshot_successful_shards gauge
						elasticsearch_snapshot_stats_snapshot_successful_shards{repository="test1",state="SUCCESS",version="5.4.2"} 10
						# HELP elasticsearch_snapshot_stats_snapshot_total_shards Last snapshot total shards
						# TYPE elasticsearch_snapshot_stats_snapshot_total_shards gauge
						elasticsearch_snapshot_stats_snapshot_total_shards{repository="test1",state="SUCCESS",version="5.4.2"} 10
						`,
		},
		{
			name: "5.4.2-failure",
			file: "../fixtures/snapshots/5.4.2-failed.json",
			want: `# HELP elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds Timestamp of the latest SUCCESS or PARTIAL snapshot
						# TYPE elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds gauge
						elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds{repository="test1"} 1.536053353e+09
						# HELP elasticsearch_snapshot_stats_number_of_snapshots Number of snapshots in a repository
						# TYPE elasticsearch_snapshot_stats_number_of_snapshots gauge
						elasticsearch_snapshot_stats_number_of_snapshots{repository="test1"} 1
						# HELP elasticsearch_snapshot_stats_oldest_snapshot_timestamp Timestamp of the oldest snapshot
						# TYPE elasticsearch_snapshot_stats_oldest_snapshot_timestamp gauge
						elasticsearch_snapshot_stats_oldest_snapshot_timestamp{repository="test1"} 1.536053353e+09
						# HELP elasticsearch_snapshot_stats_snapshot_end_time_timestamp Last snapshot end timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_end_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_end_time_timestamp{repository="test1",state="SUCCESS",version="5.4.2"} 1.536053354e+09
						# HELP elasticsearch_snapshot_stats_snapshot_failed_shards Last snapshot failed shards
						# TYPE elasticsearch_snapshot_stats_snapshot_failed_shards gauge
						elasticsearch_snapshot_stats_snapshot_failed_shards{repository="test1",state="SUCCESS",version="5.4.2"} 1
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_failures Last snapshot number of failures
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_failures gauge
						elasticsearch_snapshot_stats_snapshot_number_of_failures{repository="test1",state="SUCCESS",version="5.4.2"} 1
						# HELP elasticsearch_snapshot_stats_snapshot_number_of_indices Number of indices in the last snapshot
						# TYPE elasticsearch_snapshot_stats_snapshot_number_of_indices gauge
						elasticsearch_snapshot_stats_snapshot_number_of_indices{repository="test1",state="SUCCESS",version="5.4.2"} 2
						# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes
						# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge
						elasticsearch_snapshot_stats_snapshot_size_bytes{repository="test1",snapshot="snapshot_1"} 6024
						# HELP elasticsearch_snapshot_stats_snapshot_start_time_timestamp Last snapshot start timestamp
						# TYPE elasticsearch_snapshot_stats_snapshot_start_time_timestamp gauge
						elasticsearch_snapshot_stats_snapshot_start_time_timestamp{repository="test1",state="SUCCESS",version="5.4.2"} 1.536053353e+09
						# HELP elasticsearch_snapshot_stats_snapshot_successful_shards Last snapshot successful shards
						# TYPE elasticsearch_snapshot_stats_snapshot_successful_shards gauge
						elasticsearch_snapshot_stats_snapshot_successful_shards{repository="test1",state="SUCCESS",version="5.4.2"} 10
						# HELP elasticsearch_snapshot_stats_snapshot_total_shards Last snapshot total shards
						# TYPE elasticsearch_snapshot_stats_snapshot_total_shards gauge
						elasticsearch_snapshot_stats_snapshot_total_shards{repository="test1",state="SUCCESS",version="5.4.2"} 10
						`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(tt.file)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.RequestURI == "/_snapshot" {
					fmt.Fprint(w, `{"test1":{"type":"fs","settings":{"location":"/tmp/test1"}}}`)
					return
				}
				if strings.Contains(r.URL.Path, "/_status") {
					fmt.Fprint(w, `{"snapshots":[{"snapshot":"snapshot_1","stats":{"total":{"size_in_bytes":6024}}}]}`)
					return
				}
				io.Copy(w, f)
			}))
			defer ts.Close()

			u, err := url.Parse(ts.URL)
			if err != nil {
				t.Fatal(err)
			}

			c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
			if err != nil {
				t.Fatal(err)
			}

			if err := testutil.CollectAndCompare(wrapCollector{c}, strings.NewReader(tt.want)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotSizePerSnapshot(t *testing.T) {
	const (
		repos = `{"repo_a":{"type":"fs","settings":{"location":"/tmp/repo_a"}}}`
		snaps = `{"snapshots":[
			{"snapshot":"older","version":"8.11.0","indices":["logs"],"state":"SUCCESS","start_time_in_millis":1536053353971,"end_time_in_millis":1536053354477,"failures":[],"shards":{"total":4,"failed":0,"successful":4}},
			{"snapshot":"newer","version":"8.11.0","indices":["logs","metrics"],"state":"SUCCESS","start_time_in_millis":1536053400000,"end_time_in_millis":1536053401000,"failures":[],"shards":{"total":6,"failed":0,"successful":6}}
		]}`
		want = `# HELP elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds Timestamp of the latest SUCCESS or PARTIAL snapshot
# TYPE elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds gauge
elasticsearch_snapshot_stats_latest_snapshot_timestamp_seconds{repository="repo_a"} 1.5360534e+09
# HELP elasticsearch_snapshot_stats_number_of_snapshots Number of snapshots in a repository
# TYPE elasticsearch_snapshot_stats_number_of_snapshots gauge
elasticsearch_snapshot_stats_number_of_snapshots{repository="repo_a"} 2
# HELP elasticsearch_snapshot_stats_oldest_snapshot_timestamp Timestamp of the oldest snapshot
# TYPE elasticsearch_snapshot_stats_oldest_snapshot_timestamp gauge
elasticsearch_snapshot_stats_oldest_snapshot_timestamp{repository="repo_a"} 1.536053353e+09
# HELP elasticsearch_snapshot_stats_snapshot_end_time_timestamp Last snapshot end timestamp
# TYPE elasticsearch_snapshot_stats_snapshot_end_time_timestamp gauge
elasticsearch_snapshot_stats_snapshot_end_time_timestamp{repository="repo_a",state="SUCCESS",version="8.11.0"} 1.536053401e+09
# HELP elasticsearch_snapshot_stats_snapshot_failed_shards Last snapshot failed shards
# TYPE elasticsearch_snapshot_stats_snapshot_failed_shards gauge
elasticsearch_snapshot_stats_snapshot_failed_shards{repository="repo_a",state="SUCCESS",version="8.11.0"} 0
# HELP elasticsearch_snapshot_stats_snapshot_number_of_failures Last snapshot number of failures
# TYPE elasticsearch_snapshot_stats_snapshot_number_of_failures gauge
elasticsearch_snapshot_stats_snapshot_number_of_failures{repository="repo_a",state="SUCCESS",version="8.11.0"} 0
# HELP elasticsearch_snapshot_stats_snapshot_number_of_indices Number of indices in the last snapshot
# TYPE elasticsearch_snapshot_stats_snapshot_number_of_indices gauge
elasticsearch_snapshot_stats_snapshot_number_of_indices{repository="repo_a",state="SUCCESS",version="8.11.0"} 2
# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes
# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge
elasticsearch_snapshot_stats_snapshot_size_bytes{repository="repo_a",snapshot="newer"} 12048
elasticsearch_snapshot_stats_snapshot_size_bytes{repository="repo_a",snapshot="older"} 6024
# HELP elasticsearch_snapshot_stats_snapshot_start_time_timestamp Last snapshot start timestamp
# TYPE elasticsearch_snapshot_stats_snapshot_start_time_timestamp gauge
elasticsearch_snapshot_stats_snapshot_start_time_timestamp{repository="repo_a",state="SUCCESS",version="8.11.0"} 1.5360534e+09
# HELP elasticsearch_snapshot_stats_snapshot_successful_shards Last snapshot successful shards
# TYPE elasticsearch_snapshot_stats_snapshot_successful_shards gauge
elasticsearch_snapshot_stats_snapshot_successful_shards{repository="repo_a",state="SUCCESS",version="8.11.0"} 6
# HELP elasticsearch_snapshot_stats_snapshot_total_shards Last snapshot total shards
# TYPE elasticsearch_snapshot_stats_snapshot_total_shards gauge
elasticsearch_snapshot_stats_snapshot_total_shards{repository="repo_a",state="SUCCESS",version="8.11.0"} 6
`
	)

	es := &fakeSnapshotStatus{
		t:     t,
		repo:  "repo_a",
		sizes: map[string]int64{"older": 6024, "newer": 12048},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_snapshot":
			fmt.Fprint(w, repos)
		case strings.HasSuffix(r.URL.Path, "/_status"):
			es.ServeHTTP(w, r)
		default:
			fmt.Fprint(w, snaps)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := testutil.CollectAndCompare(wrapCollector{c}, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// fakeSnapshotStatus mimics GET /_snapshot/{repo}/{names}/_status. Like Elasticsearch,
// _all returns only running snapshots, so finished ones are visible only by name.
type fakeSnapshotStatus struct {
	t           *testing.T
	repo        string
	sizes       map[string]int64
	running     map[string]bool
	state       map[string]string
	calls       [][]string
	failBatches int
}

func (f *fakeSnapshotStatus) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if got, want := r.URL.Query().Get("filter_path"), "snapshots.snapshot,snapshots.state,snapshots.stats.total.size_in_bytes"; got != want {
		f.t.Errorf("filter_path = %q, want %q", got, want)
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/_snapshot/"+f.repo+"/"), "/")
	names := strings.Split(parts[0], ",")
	f.calls = append(f.calls, names)
	if f.failBatches > 0 {
		f.failBatches--
		http.Error(w, "status unavailable", http.StatusInternalServerError)
		return
	}

	var entries []string
	for _, name := range names {
		if name == "_all" {
			for n := range f.running {
				entries = append(entries, f.entry(n))
			}
			continue
		}
		if _, ok := f.sizes[name]; !ok {
			http.Error(w, `{"error":{"type":"snapshot_missing_exception"}}`, http.StatusNotFound)
			return
		}
		entries = append(entries, f.entry(name))
	}
	fmt.Fprintf(w, `{"snapshots":[%s]}`, strings.Join(entries, ","))
}

func (f *fakeSnapshotStatus) entry(name string) string {
	state := "SUCCESS"
	if f.running[name] {
		state = "STARTED"
	}
	if s, ok := f.state[name]; ok {
		state = s
	}
	return fmt.Sprintf(`{"snapshot":%q,"state":%q,"stats":{"total":{"size_in_bytes":%d}}}`, name, state, f.sizes[name])
}

func TestSnapshotSizeCachesFinishedSnapshots(t *testing.T) {
	list := func(names ...string) string {
		var s []string
		for _, n := range names {
			s = append(s, fmt.Sprintf(`{"snapshot":%q,"version":"7.17.26","indices":["logs"],"state":"SUCCESS","start_time_in_millis":1536053353971,"end_time_in_millis":1536053354477,"failures":[],"shards":{"total":1,"failed":0,"successful":1}}`, n))
		}
		return `{"snapshots":[` + strings.Join(s, ",") + `]}`
	}

	es := &fakeSnapshotStatus{
		t:       t,
		repo:    "repo_a",
		sizes:   map[string]int64{"done": 100, "running": 5},
		running: map[string]bool{"running": true},
	}
	listed := list("done", "running")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_snapshot":
			fmt.Fprint(w, `{"repo_a":{"type":"fs","settings":{"location":"/tmp/repo_a"}}}`)
		case strings.HasSuffix(r.URL.Path, "/_status"):
			es.ServeHTTP(w, r)
		default:
			fmt.Fprint(w, listed)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	sizes := func() string {
		var b strings.Builder
		b.WriteString("# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes\n")
		b.WriteString("# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge\n")
		for _, n := range []string{"done", "new", "running"} {
			if size, ok := es.sizes[n]; ok && strings.Contains(listed, `"`+n+`"`) {
				fmt.Fprintf(&b, "elasticsearch_snapshot_stats_snapshot_size_bytes{repository=\"repo_a\",snapshot=%q} %d\n", n, size)
			}
		}
		return b.String()
	}
	collect := func() {
		t.Helper()
		if err := testutil.CollectAndCompare(wrapCollector{c}, strings.NewReader(sizes()), "elasticsearch_snapshot_stats_snapshot_size_bytes"); err != nil {
			t.Fatal(err)
		}
	}

	collect()
	if got := fmt.Sprint(es.calls); got != "[[done] [running]]" {
		t.Fatalf("first scrape status calls = %s, want one call per snapshot", got)
	}

	es.calls = nil
	es.sizes["running"] = 50
	collect()
	if got := fmt.Sprint(es.calls); got != "[[running]]" {
		t.Fatalf("second scrape status calls = %s, want only the running snapshot", got)
	}

	es.calls = nil
	delete(es.running, "running")
	es.sizes["new"] = 7
	listed = list("running", "new")
	collect()
	if got := fmt.Sprint(es.calls); got != "[[running] [new]]" {
		t.Fatalf("third scrape status calls = %s, want one call per snapshot", got)
	}

	es.calls = nil
	collect()
	if len(es.calls) != 0 {
		t.Fatalf("fourth scrape status calls = %v, want none once every snapshot is finished", es.calls)
	}

	es.calls = nil
	listed = list("done")
	collect()
	if got := fmt.Sprint(es.calls); got != "[[done]]" {
		t.Fatalf("relisting a dropped snapshot status calls = %s, want [[done]]", got)
	}
}

func TestSnapshotSizeOneNamePerRequest(t *testing.T) {
	const n = 4
	var listed []string
	es := &fakeSnapshotStatus{t: t, repo: "repo_a", sizes: map[string]int64{}}
	for i := range n {
		name := fmt.Sprintf("snap_%02d", i)
		es.sizes[name] = int64(i)
		listed = append(listed, fmt.Sprintf(`{"snapshot":%q,"state":"SUCCESS","start_time_in_millis":%d,"shards":{}}`, name, i))
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_snapshot":
			fmt.Fprint(w, `{"repo_a":{"type":"fs","settings":{"location":"/tmp/repo_a"}}}`)
		case strings.HasSuffix(r.URL.Path, "/_status"):
			es.ServeHTTP(w, r)
		default:
			fmt.Fprint(w, `{"snapshots":[`+strings.Join(listed, ",")+`]}`)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if got := testutil.CollectAndCount(wrapCollector{c}, "elasticsearch_snapshot_stats_snapshot_size_bytes"); got != n {
		t.Fatalf("size metrics = %d, want %d", got, n)
	}
	if len(es.calls) != n {
		t.Fatalf("status calls = %d %v, want %d calls of one name", len(es.calls), es.calls, n)
	}
	for _, call := range es.calls {
		if len(call) != 1 {
			t.Fatalf("status call %v, want one snapshot name", call)
		}
	}
	if es.calls[0][0] != "snap_03" {
		t.Fatalf("first status call = %s, want the newest snapshot", es.calls[0][0])
	}
}

func TestSnapshotSizeCachesOnlyTerminalStates(t *testing.T) {
	es := &fakeSnapshotStatus{
		t:     t,
		repo:  "repo_a",
		sizes: map[string]int64{"init": 0, "aborted": 9},
		state: map[string]string{"init": "INIT", "aborted": "ABORTED"},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_snapshot":
			fmt.Fprint(w, `{"repo_a":{"type":"fs","settings":{"location":"/tmp/repo_a"}}}`)
		case strings.HasSuffix(r.URL.Path, "/_status"):
			es.ServeHTTP(w, r)
		default:
			fmt.Fprint(w, `{"snapshots":[
				{"snapshot":"init","version":"7.17.26","state":"IN_PROGRESS","indices":["logs"],"start_time_in_millis":1536053353971,"end_time_in_millis":0,"failures":[],"shards":{"total":1,"failed":0,"successful":0}},
				{"snapshot":"aborted","version":"7.17.26","state":"FAILED","indices":["logs"],"start_time_in_millis":1536053353971,"end_time_in_millis":1536053354477,"failures":[],"shards":{"total":1,"failed":1,"successful":0}}
			]}`)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	collect := func(initSize int64) {
		t.Helper()
		if err := testutil.CollectAndCompare(wrapCollector{c}, strings.NewReader(fmt.Sprintf(`
# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes
# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge
elasticsearch_snapshot_stats_snapshot_size_bytes{repository="repo_a",snapshot="aborted"} 9
elasticsearch_snapshot_stats_snapshot_size_bytes{repository="repo_a",snapshot="init"} %d
`, initSize)), "elasticsearch_snapshot_stats_snapshot_size_bytes"); err != nil {
			t.Fatal(err)
		}
	}
	collect(0)
	es.calls = nil
	es.sizes["init"] = 4
	collect(4)
	if got := fmt.Sprint(es.calls); got != "[[init]]" {
		t.Fatalf("second scrape status calls = %s, want [[init]]", got)
	}
}

func TestSnapshotSizeBatchFailureContinues(t *testing.T) {
	const n = 4
	var listed []string
	es := &fakeSnapshotStatus{t: t, repo: "repo_a", sizes: map[string]int64{}, failBatches: 1}
	for i := range n {
		name := fmt.Sprintf("snap_%02d", i)
		es.sizes[name] = int64(i + 1)
		listed = append(listed, fmt.Sprintf(`{"snapshot":%q,"state":"SUCCESS","shards":{}}`, name))
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_snapshot":
			fmt.Fprint(w, `{"repo_a":{"type":"fs","settings":{"location":"/tmp/repo_a"}}}`)
		case strings.HasSuffix(r.URL.Path, "/_status"):
			es.ServeHTTP(w, r)
		default:
			fmt.Fprint(w, `{"snapshots":[`+strings.Join(listed, ",")+`]}`)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if got := testutil.CollectAndCount(wrapCollector{c}, "elasticsearch_snapshot_stats_snapshot_size_bytes"); got != n-1 {
		t.Fatalf("size metrics = %d, want every snapshot except the one status call that failed", got)
	}
	if len(es.calls) != n {
		t.Fatalf("status calls = %v, want %d single-snapshot calls", es.calls, n)
	}
}

func TestSnapshotSizeStatusFailureKeepsCachedSize(t *testing.T) {
	es := &fakeSnapshotStatus{t: t, repo: "repo_a", sizes: map[string]int64{"done": 100, "fresh": 7}}
	listed := `{"snapshots":[{"snapshot":"done","version":"7.17.26","indices":["logs"],"state":"SUCCESS","start_time_in_millis":1536053353971,"end_time_in_millis":1536053354477,"failures":[],"shards":{"total":1,"failed":0,"successful":1}}]}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_snapshot":
			fmt.Fprint(w, `{"repo_a":{"type":"fs","settings":{"location":"/tmp/repo_a"}}}`)
		case strings.HasSuffix(r.URL.Path, "/_status"):
			es.ServeHTTP(w, r)
		default:
			fmt.Fprint(w, listed)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if got := testutil.CollectAndCount(wrapCollector{c}, "elasticsearch_snapshot_stats_snapshot_size_bytes"); got != 1 {
		t.Fatalf("first scrape size metrics = %d, want 1", got)
	}

	es.failBatches = 1
	listed = `{"snapshots":[
		{"snapshot":"done","version":"7.17.26","indices":["logs"],"state":"SUCCESS","start_time_in_millis":1536053353971,"end_time_in_millis":1536053354477,"failures":[],"shards":{"total":1,"failed":0,"successful":1}},
		{"snapshot":"fresh","version":"7.17.26","indices":["logs"],"state":"SUCCESS","start_time_in_millis":1536053400000,"end_time_in_millis":1536053401000,"failures":[],"shards":{"total":1,"failed":0,"successful":1}}
	]}`
	if err := testutil.CollectAndCompare(wrapCollector{c}, strings.NewReader(`
# HELP elasticsearch_snapshot_stats_snapshot_size_bytes Total size of each snapshot in bytes
# TYPE elasticsearch_snapshot_stats_snapshot_size_bytes gauge
elasticsearch_snapshot_stats_snapshot_size_bytes{repository="repo_a",snapshot="done"} 100
`), "elasticsearch_snapshot_stats_snapshot_size_bytes"); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSizeStatusFailureKeepsOtherMetrics(t *testing.T) {
	body, err := os.ReadFile("../fixtures/snapshots/5.4.2.json")
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.RequestURI == "/_snapshot" {
			fmt.Fprint(w, `{"test1":{"type":"fs","settings":{"location":"/tmp/test1"}}}`)
			return
		}
		if strings.Contains(r.URL.Path, "/_status") {
			http.Error(w, "status unavailable", http.StatusInternalServerError)
			return
		}
		w.Write(body)
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}

	if got := testutil.CollectAndCount(wrapCollector{c}, "elasticsearch_snapshot_stats_snapshot_size_bytes"); got != 0 {
		t.Fatalf("size metrics = %d, want 0 when status fetch fails", got)
	}
	if got := testutil.CollectAndCount(wrapCollector{c}, "elasticsearch_snapshot_stats_number_of_snapshots"); got != 1 {
		t.Fatalf("number_of_snapshots = %d, want 1 when status fetch fails", got)
	}
}

func TestSnapshotStatusFetchDoesNotBlockScrape(t *testing.T) {
	snapshotStatusAsync = true
	t.Cleanup(func() { snapshotStatusAsync = false })

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(release) }) }
	es := &fakeSnapshotStatus{t: t, repo: "repo_a", sizes: map[string]int64{"done": 100}}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_snapshot":
			fmt.Fprint(w, `{"repo_a":{"type":"fs","settings":{"location":"/tmp/repo_a"}}}`)
		case strings.HasSuffix(r.URL.Path, "/_status"):
			once.Do(func() { close(started) })
			<-release
			es.ServeHTTP(w, r)
		default:
			fmt.Fprint(w, `{"snapshots":[{"snapshot":"done","version":"7.17.26","state":"SUCCESS","start_time_in_millis":1536053400000,"end_time_in_millis":1536053401000,"indices":["logs"],"failures":[],"shards":{"total":1,"failed":0,"successful":1}}]}`)
		}
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(releaseClose)

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewSnapshots(promslog.NewNopLogger(), u, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.(*Snapshots).stopStatusFetch)

	start := time.Now()
	if got := testutil.CollectAndCount(wrapCollector{c}, "elasticsearch_snapshot_stats_snapshot_size_bytes"); got != 0 {
		t.Fatalf("first scrape size metrics = %d, want 0 while the status call is still running", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("scrape took %s; the status call blocked it", elapsed)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("background status fetch did not start")
	}
	releaseClose()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if testutil.CollectAndCount(wrapCollector{c}, "elasticsearch_snapshot_stats_snapshot_size_bytes") == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cached size was not emitted on a later scrape")
}
