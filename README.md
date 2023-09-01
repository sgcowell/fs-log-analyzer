# Dremio FileSystem Log Analyzer

## Prerequisites

* Python 3
* The following loggers must be enabled in your logback.xml:

| Logger | Level |
| -------|-------|
| com.dremio.exec.work.foreman.AttemptManager | DEBUG |
| com.amazonaws.request | DEBUG |
| software.amazon.awssdk.request | DEBUG |
| query.logger | INFO (this is the default) |
| com.dremio.exec.store.dfs.LoggedFileSystem | TRACE |


## Usage

Jobs MUST be run in isolation for proper attribution of IOs. Any concurrent activity on your Dremio instance may result in IOs not associated to the requested job being included in the report.

```
python analyze-filesystem-logs-for-job.py [-h] [-t TIME_SCALE] [-o OUTPUT_FILE] [-m MIN_ELAPSED] job_id log_dir

positional arguments:
  job_id                Job ID
  log_dir               Root directory of the log folder to scan.  There must be a json/server.json file in this directory.

options:
  -h, --help            show this help message and exit
  -t TIME_SCALE, --time-scale TIME_SCALE
                        Time scale for the visualization.  This is a decimal value representing the number of ms of elapsed time per pixel.
  -o OUTPUT_FILE, --output-file OUTPUT_FILE
                        Output file for the HTML report.  If not specified, a temporary file location will be used.
  -m MIN_ELAPSED, --min-elapsed MIN_ELAPSED
                        Min elapsed time.  FS calls under this threshold will be filtered out.
```

Example:

```
python analyze-filesystem-logs-for-job.py 1b0dfe2b-108f-1678-46ec-3fb7abb6e700 ~/dremio/main/log
```