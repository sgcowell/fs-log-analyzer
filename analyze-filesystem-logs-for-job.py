import argparse
import collections
import datetime
import json
import os
import re
import sys
import tempfile
import webbrowser

ATTEMPT_MANAGER_LOGGER = 'com.dremio.exec.work.foreman.AttemptManager'
FILESYSTEM_LOGGER = 'com.dremio.exec.store.dfs.LoggedFileSystem'
AWS_V1_REQUEST_LOGGER = 'com.amazonaws.request'
AWS_V2_REQUEST_LOGGER = 'software.amazon.awssdk.request'
QUERY_LOGGER = 'query.logger'

def timestamp(msg):
    return datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S,%f')

Event = collections.namedtuple('Event', [ 'layer', 'thread', 'resource', 'op', 'start_ms', 'elapsed_ms' ])

html_template = '''<!DOCTYPE html>
<html>
  <body>
    <h2 style='font-family: sans-serif;'>{}</h2>
    <div id="container"></div>
    <script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
    <script type="module">

const width  = window.innerWidth || document.documentElement.clientWidth || document.body.clientWidth;
const height = window.innerHeight || document.documentElement.clientHeight || document.body.clientHeight;

const all_data = [
{}
]

const data = all_data.filter(e => e.layer === 'FS')

const threads = d3.groupSort(data, ([d]) => -d.start_ms, (d) => d.thread)
const files = d3.groupSort(data, ([d]) => -d.start_ms, (d) => d.resource)
const ops = [ 'open', 'read',  'asyncRead.complete', 'getFileAttributes', 'flush', 'create', 'write', 'close', 'exists', 'isDirectory', 'isFile', 'listFiles', 'listFiles.hasNext' ]
const query_end_ms = d3.max(data, (d) => d.start_ms + d.elapsed_ms)

// Declare the chart dimensions and margins.
const time_scale = {}
const yAxisWidth = 400;

const marginTop = 30;
const marginRight = 5;
const marginBottom = 30;
const marginLeft = 5;
const chartWidth = Math.max(query_end_ms / time_scale, 400) + marginLeft + marginRight
const chartHeight = files.length * 30 + marginTop + marginBottom;
const visibleChartWidth = Math.min(chartWidth, width - yAxisWidth)

// Legend definitions
const legendIconSize = 10;
const legendColWidth = 150;
const legendRowHeight = legendIconSize * 2;
const legendMargin = 10;
const legendItemsPerCol = 4;
const legendNumCols = Math.ceil(ops.length / legendItemsPerCol);
const legendWidth = yAxisWidth + visibleChartWidth;
const legendHeight = (legendMargin * 2) + (legendRowHeight * (legendItemsPerCol + 1));
const legendLeftOffset = yAxisWidth + ((visibleChartWidth - (legendNumCols * legendColWidth)) / 2);

// Declare the x (horizontal position) scale.
const x = d3.scaleLinear()
  .domain([0, query_end_ms])
  .range([marginLeft, chartWidth - marginRight]);

// Declare the y scale.
const y = d3.scaleBand()
  .domain(files)
  .range([chartHeight - marginBottom, marginTop])
  .padding(0.25)

const color = d3.scaleOrdinal()
  .domain(ops)
  .range(d3.schemePaired)

const parent = d3.create("div");

// Create a div that holds two svg elements: one for the main chart and horizontal axis,
// which moves as the user scrolls the content; the other for the vertical axis (which 
// doesn't scroll).
const chartDiv = parent.append("div")
  .attr("style", "display: flex;");

// Div to hold y axis
const yAxisDiv = chartDiv.append("div")
  .attr("style", `flex: 0 0 ${{yAxisWidth}}px;`);

// Create the fixed SVG container.
const fixedSvg = yAxisDiv.append("svg")
  .attr("width", yAxisWidth)
  .attr("height", chartHeight)
  .attr("viewBox", [0, 0, yAxisWidth, chartHeight])
  .attr("style", "max-width: 100%; height: auto;");

// Add the y-axis and label, and remove the domain line.
fixedSvg.append("g")
  .attr("transform", `translate(${{yAxisWidth}},0)`)
  .call(d3.axisLeft(y).tickSizeOuter(0).tickSizeInner(0))

// Create a scrolling div containing the area shape and the horizontal axis. 
const body = chartDiv.append("div")
  .attr("style", "flex-grow: 1; overflow-x: auto;");

const svg = body.append("svg")
  .attr("width", chartWidth)
  .attr("height", chartHeight)
  .style("display", "block");

// Add a rect for each bar.
const bars = svg.append("g")
  .selectAll()
  .data(data);

bars.join("rect")
    .attr("x", (d) => x(d.start_ms))
    .attr("y", (d) => y(d.resource))
    .attr("height", y.bandwidth())
    .attr("width", (d) => x(d.start_ms + d.elapsed_ms) - x(d.start_ms))
    .attr("fill", (d) => color(d.op));

bars.join("text")
    .attr("x", (d) => x(d.start_ms + d.elapsed_ms) - 4)
    .attr("y", (d) => y(d.resource) + y.bandwidth() - 7)
    .attr("font-family", "sans-serif")
    .attr("font-size", "8pt")
    .style("text-anchor", "end")
    .style("fill", "white")
    .text((d) => d.elapsed_ms > 16 * time_scale ? d.elapsed_ms : null);

// Add the x-axis and label.
svg.append("g")
  .attr("transform", `translate(0,${{chartHeight - marginBottom}})`)
  .call(d3.axisBottom(x).tickSizeOuter(0));

// create a div + svg to hold the legend at the bottom
const legendSvg = parent.append("div").append("svg")
  .attr("width", legendWidth)
  .attr("height", legendHeight)
  .attr("viewBox", [0, 0, legendWidth, legendHeight])
  .attr("style", "max-width: 100%; height: auto;");

// Add an x axis label
legendSvg.append("text")
    .attr("x", yAxisWidth + (Math.min(chartWidth, width - yAxisWidth) / 2))
    .attr("y", legendMargin)
    .attr("text-anchor", "middle")
    .attr("font-family", "sans-serif")
    .attr("font-size", "8pt")
    .style("alignment-baseline", "middle")
    .text("Elapsed (ms)")


// Add one dot in the legend for each name.
legendSvg.selectAll("legend-marks")
  .data(ops)
  .enter()
  .append("rect")
    .attr("x", (d, i) => legendLeftOffset + (Math.floor(i / 4) * legendColWidth))
    .attr("y", (d, i) => legendMargin + (((i % 4) + 1) * legendRowHeight))
    .attr("height", legendIconSize)
    .attr("width", legendIconSize)
    .style("fill", (d) => color(d))

// Add the legend text labels
legendSvg.selectAll("legend-text")
  .data(ops)
  .enter()
  .append("text")
    .attr("x", (d, i) => legendLeftOffset + (legendIconSize * 2) + (Math.floor(i / 4) * legendColWidth))
    .attr("y", (d, i) => legendMargin + (legendIconSize / 2) + (((i % 4) + 1) * legendRowHeight))
    .text((d) => d)
    .attr("text-anchor", "left")
    .attr("font-family", "sans-serif")
    .attr("font-size", "8pt")
    .style("alignment-baseline", "middle")

container.append(parent.node())

// add table of the data
const columns = [ 'layer', 'thread', 'resource', 'op', 'start_ms', 'elapsed_ms' ]
const widths = [ 'auto', 'auto', 'auto', 'auto', 'auto', 'auto' ]

const table = d3.create('table')
  .style('font-family', 'sans-serif')
  .style('font-size', '10pt')
  .style('border', '1px solid darkgray')
  .style('border-collapse', 'collapse')
  .style('margin-top', '40px')
  .style('margin-left', '10px');

table.append('colgroup')
  .selectAll('col')
  .data(columns)
  .enter()
  .append('col')
  .style('width', (c, i) => widths[i]);

const thead = table.append('thead');
const tbody = table.append('tbody');

// append the header row
thead.append('tr')
  .selectAll('th')
  .data(columns).enter()
  .append('th')
  .style('border', '1px solid darkgray')
  .style('border-collapse', 'collapse')
  .style('padding', '4px 10px 4px 10px')
  .text((c) => c);

// create a row for each object in the data
const rows = tbody.selectAll('tr')
  .data(all_data)
  .enter()
  .append('tr');

// create a cell in each row for each column
const cells = rows.selectAll('td')
  .data((r) => columns.map((c) => {{ return {{ column: c, value: r[c] }}}}))
  .enter()
  .append('td')
  .style('border', '1px solid darkgray')
  .style('border-collapse', 'collapse')
  .style('padding', '4px 10px 4px 10px')
  .style('text-align', (d) => d.column === 'start_ms' || d.column == 'elapsed_ms' ? 'right' : 'left')
  .text((d) => d.value);

container.append(table.node())

    </script>
  </body>
</html>
'''

class LogAnalyzer:
    def __init__(self, job_id, log_file, min_elapsed):
        self.job_id = job_id
        self.log_file = log_file
        self.min_elapsed = min_elapsed
        self.job_start_msg = None
        self.job_start_ts = None
        self.job_end_msg = None
        self.aws_request_start_msgs = {}
        self.events = []
        self.sql = None

    def is_planning_thread(self, msg):
        return self.job_id in msg['thread'] and 'foreman' in msg['thread']

    def is_job_thread(self, msg):
        return self.job_id in msg['thread']

    def is_msg_for_job(self, msg):
        msg_ts = timestamp(msg)
        return (self.job_start_ts is not None and
                self.job_end_msg is None and 
                msg_ts >= self.job_start_ts and 
                (self.job_id in msg['thread'] or 
                 's3a-transfer' in msg['thread'] or 
                 'manifest-writers' in msg['thread'] or 
                 's3-async' in msg['thread'] or
                 'delta-metadata-fetch' in msg['thread'] or
                 (msg['logger'] == QUERY_LOGGER and msg['queryId'] == self.job_id)))

    def is_aws_v1_request(self, msg):
        return msg['logger'] == AWS_V1_REQUEST_LOGGER and 'Sending Request:' in msg['message']

    def is_aws_v2_request(self, msg):
        return msg['logger'] == AWS_V2_REQUEST_LOGGER and 'Sending Request:' in msg['message']

    def check_for_end_msg(self, msg):
        if self.job_end_msg is None and msg['logger'] == QUERY_LOGGER and msg['queryId'] == self.job_id:
            self.job_end_msg = msg
            self.sql = msg['queryText']

    def analyze(self):
        with open(log_file) as f:
            for line in f:
                msg = json.loads(line)
                ts = timestamp(msg)
                thread = msg['thread']

                # check for job start
                if self.job_start_msg is None and self.is_planning_thread(msg):
                    self.job_start_msg = msg
                    self.job_start_ts = timestamp(msg)

                if self.is_msg_for_job(msg):
                    if msg['logger'] == FILESYSTEM_LOGGER:
                        match = re.search('(\S+) elapsed=(\d+)ms scheme=(\w+) path=(\S+)', msg['message'])
                        if match is not None:
                            op = match.group(1)
                            elapsed_ms = int(match.group(2))
                            path = match.group(4)
                            (dir, file) = os.path.split(path)
                            (root_dir, sub_dir) = os.path.split(dir)
                            rel_path = os.path.join(sub_dir, file)
                            start_ms = int((ts - self.job_start_ts) / datetime.timedelta(milliseconds=1)) - elapsed_ms
                            
                            if elapsed_ms >= self.min_elapsed:
                                event = Event('FS', thread, rel_path, op, start_ms, elapsed_ms)
                                self.events.append(event)
                    elif self.is_aws_v1_request(msg) or self.is_aws_v2_request(msg):
                        if self.is_aws_v1_request(msg):
                            match = re.search('Sending Request: (\w+) \S+ (\S+)', msg['message'])
                        else:
                            match = re.search('Sending Request: DefaultSdkHttpFullRequest\(httpMethod=(\w+).* encodedPath=([^ ,]+)', msg['message'])

                        op = match.group(1)
                        resource = match.group(2)
                        start_ms = int((ts - self.job_start_ts) / datetime.timedelta(milliseconds=1))
                        (dir, file) = os.path.split(resource)
                        (root_dir, sub_dir) = os.path.split(dir)
                        rel_path = os.path.join(sub_dir, file)

                        event = Event('S3', thread, rel_path, op, start_ms, None)
                        self.events.append(event)
                    else:
                        self.check_for_end_msg(msg)

                if self.job_end_msg is not None:
                    return True
                
        return False
            

    def write_html_report(self, time_scale, output_file):
        self.events.sort(key=lambda e: e.start_ms * 10 + (0 if e.layer == 'FS' else 1))
        data_rows = []
        for e in self.events:
            data_rows.append(json.dumps(e._asdict()))
        
        joined_rows = '  ' + ',\n  '.join(data_rows)
        output_file.write(html_template.format(self.sql, joined_rows, time_scale))

        
required_loggers = """
Jobs MUST be run in isolation for correct analysis by this script.  Concurrent activity may result in IOs being
incorectly attributed to the job being analyzed.  Do NOT use this script to anlyze jobs on production clusters.

Analysis requires the following loggers to be enabled:

com.dremio.exec.work.foreman.AttemptManager   DEBUG
com.amazonaws.request                         DEBUG
software.amazon.awssdk.request                DEBUG
query.logger                                  INFO
com.dremio.exec.store.dfs.LoggedFileSystem    TRACE
"""

parser = argparse.ArgumentParser(prog='analyze-filesystem-logs-for-job', epilog=required_loggers, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("job_id", help="Job ID")
parser.add_argument("log_dir", help="Root directory of the log folder to scan.  There must be a json/server.json file in this directory.")
parser.add_argument("-t", "--time-scale", default='3', type=float, help="Time scale for the visualization.  This is a decimal value representing the number of ms of elapsed time per pixel.")
parser.add_argument("-o", "--output-file", help="Output file for the HTML report.  If not specified, a temporary file location will be used.")
parser.add_argument("-m", "--min-elapsed", default=1, type=int, help="Min elapsed time.  FS calls under this threshold will be filtered out.")

args = parser.parse_args()
log_file = os.path.abspath(os.path.join(args.log_dir, 'json', 'server.json'))
if not os.path.exists(log_file):
    print("Could not find logs at {}".format(log_file), file=sys.stderr)
    sys.exit(1)

analyzer = LogAnalyzer(args.job_id, log_file, args.min_elapsed)
success = analyzer.analyze()

if success:
    if args.output_file is None:
        output_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".html")
    else:
        output_file = open(args.output_file, "w", encoding="utf-8")

    with output_file:
        print("Output file: {}".format(output_file.name))
        analyzer.write_html_report(args.time_scale, output_file)

    webbrowser.open("file://" + os.path.abspath(output_file.name))
else:
    print("Could not find relevant log messages for job_id {} in {}".format(args.job_id, log_file))

