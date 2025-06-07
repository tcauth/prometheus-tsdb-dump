# prometheus-tsdb-dump

prometheus-tsdb-dump reads a Prometheus TSDB block and writes metrics.

## Installation

```
$ git clone https://github.com/ryotarai/prometheus-tsdb-dump.git
$ cd prometheus-tsdb-dump
$ make build
```

## Usage

```
$ prometheus-tsdb-dump -block /path/to/prometheus-data/block-ulid -format victoriametrics
{"metric":{"__name__":"up","job":"node-exporter","instance":"a"},"values":[1,1,1],"timestamps":[1578636058619,1578636118619,1578636178619]}
{"metric":{"__name__":"up","job":"node-exporter","instance":"b"},"values":[1,1,1],"timestamps":[1578636058619,1578636118619,1578636178619]}
```

## Options

- `-format`: Output format (default: victoriametrics)
- `-min-timestamp`: Minimum timestamp of exported samples (unix time in msec)
- `-max-timestamp`: Maximum timestamp of exported samples (unix time in msec)
- The `-block` path can point to a local directory or an `s3://` location.
- `-dump-index`: Dump block index information. The block path can point to a
  local directory or an `s3://` location.
- `-aws-profile`: AWS profile to use when accessing S3 for `-dump-index` or
  when reading a block from S3 with `-block`
- `-output`: Write output to the given file instead of stdout
- `-label-value`: Comma-separated list of label values to filter by

## Output Formats

Output format can be configured via `-format` option.

### `victoriametrics`

`victoriametrics` format is a JSON streaming format which can be imported via [VictoriaMetrics' `/api/v1/import` API](https://github.com/VictoriaMetrics/VictoriaMetrics#how-to-import-time-series-data). It looks like:

```
{"metric":{"__name__":"up","job":"node-exporter"},"values":[1,1,1],"timestamps":[1578636058619,1578636118619,1578636178619]}
```

#### How to import TSDB data from Prometheus to VictoriaMetrics

First, take a snapshot of Prometheus TSDB:

```
$ curl -XPOST http://your-prometheus:9090/api/v1/admin/tsdb/snapshot
{"status":"success","data":{"name":"20200110T104512Z-xxxxxxxxxxxx"}}
```

Then, import data to VictoriaMetrics:

```
$ cd /path/to/prometheus/data/snapshots/20200110T104512Z-xxxxxxxxxxxx
$ parallelism="$(nproc)"
$ find . -mindepth 1 -maxdepth 1 -type d | xargs -n1 -P "$parallelism" sh -c 'echo $0; prometheus-tsdb-dump-linux -block "$0" -format victoriametrics | curl http://your-victoriametrics:8428/api/v1/import -T -'
```
