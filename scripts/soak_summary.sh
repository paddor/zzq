#!/bin/sh
# Summarize completed nextest runs without changing their pass/fail result.
set -eu

if [ $# -eq 0 ]; then
	echo 'usage: scripts/soak_summary.sh RUN_DIRECTORY...' >&2
	exit 1
fi

summarize() {
	dir=$1
	if [ ! -f "$dir/soak.xml" ]; then
		echo "no report: $dir/soak.xml" >&2
		return 1
	fi
	# The samples of each job, then the report that lists the jobs.
	set -- "$dir"/*/resources.jsonl
	[ -e "$1" ] || set --
	DIR=$dir RUN=$(cd "$dir" && pwd) XML=$dir/soak.xml awk "$PROGRAM" "$@" "$dir/soak.xml"
}

PROGRAM='
function number(key) {
	return match($0, "\"" key "\":[0-9]+") ? substr($0, RSTART + length(key) + 3, RLENGTH - length(key) - 3) + 0 : 0
}

function attr(line, key) {
	if (!match(line, " " key "=\"[^\"]*\""))
		return ""
	return substr(line, RSTART + length(key) + 3, RLENGTH - length(key) - 4)
}

function xml_text(s) {
	gsub(/&lt;/, "<", s)
	gsub(/&gt;/, ">", s)
	gsub(/&quot;/, "\"", s)
	gsub(/&apos;/, "\047", s)
	gsub(/&#(10|[xX][aA]);/, "\n", s)
	gsub(/&#(9|[xX]9);/, "\t", s)
	gsub(/&amp;/, "\\&", s)
	return s
}

function json_string(s) {
	gsub(/\\/, "\\\\", s)
	gsub(/"/, "\\\"", s)
	gsub(/\n/, "\\n", s)
	gsub(/\t/, "\\t", s)
	gsub(/[\001-\037]/, " ", s)
	return "\"" s "\""
}

function mean_in(key, from, to,    i, sum, n) {
	sum = 0
	n = 0
	for (i = 1; i <= samples[key]; i++) {
		if (seconds[key, i] >= from && seconds[key, i] <= to) {
			sum += rss[key, i]
			n++
		}
	}
	return n ? sum / n : ""
}

# Average windows avoid a single allocator spike or teardown sample.
function growth(key,    first, last, before, after) {
	first = seconds[key, 1] + 60
	last = seconds[key, samples[key]] - 2
	if (last - first < 60)
		return ""
	before = mean_in(key, first, first + 29)
	after = mean_in(key, last - 29, last)
	if (before == "" || after == "")
		return ""
	return sprintf("%.2f", (after - before) / 1048576)
}

function write_resources(i,    p, key, peak, grown, more) {
	job_peak = ""
	job_growth = ""
	for (p = 1; p <= pids[name[i]]; p++) {
		key = name[i] SUBSEP pid_of[name[i], p]
		peak = sprintf("%.2f", peak_rss[key] / 1048576)
		grown = growth(key)
		if (job_peak == "" || peak + 0 > job_peak + 0)
			job_peak = peak
		if (grown != "" && (job_growth == "" || grown + 0 > job_growth + 0))
			job_growth = grown
		more = p < pids[name[i]] ? "," : ""
		printf "        {\n" > OUT
		printf "          \"pid\": %.0f,\n", pid_of[name[i], p] > OUT
		printf "          \"seconds\": %.0f,\n", seconds[key, samples[key]] - seconds[key, 1] > OUT
		printf "          \"peak_rss_mib\": %s,\n", peak > OUT
		printf "          \"rss_growth_after_warmup_mib\": %s,\n", (grown == "" ? "null" : grown) > OUT
		printf "          \"peak_fds\": %.0f,\n", peak_fds[key] > OUT
		printf "          \"peak_threads\": %.0f,\n", peak_threads[key] > OUT
		printf "          \"progress\": %.0f\n", progress[key] > OUT
		printf "        }%s\n", more > OUT
	}
}

BEGIN {
	OUT = ENVIRON["DIR"] "/summary.json"
	xml = ENVIRON["XML"]
}

FILENAME != xml {
	if (!match($0, /"pid":[0-9]+/))
		next
	job = FILENAME
	sub(/\/resources\.jsonl$/, "", job)
	sub(/.*\//, "", job)
	sub(/-[0-9]+$/, "", job)
	if (!(job in source))
		source[job] = FILENAME
	if (source[job] != FILENAME)
		next
	pid = number("pid")
	key = job SUBSEP pid
	if (!(key in samples))
		pid_of[job, ++pids[job]] = pid
	n = ++samples[key]
	seconds[key, n] = number("seconds")
	rss[key, n] = number("rss")
	if (rss[key, n] > peak_rss[key])
		peak_rss[key] = rss[key, n]
	if (number("fds") > peak_fds[key])
		peak_fds[key] = number("fds")
	if (number("threads") > peak_threads[key])
		peak_threads[key] = number("threads")
	progress[key] = number("progress")
	next
}

/^[ \t]*<testcase / {
	jobs++
	name[jobs] = attr($0, "name")
	elapsed[jobs] = attr($0, "time")
	status[jobs] = "passed"
	message[jobs] = ""
	failed[jobs] = 0
}

/^[ \t]*<(failure|error)[ \/>]/ && !failed[jobs] {
	failed[jobs] = 1
	status[jobs] = attr($0, "type") == "test abort" ? "aborted" : "failed"
	message[jobs] = xml_text(attr($0, "message"))
}

/^[ \t]*<skipped[ \/>]/ && !failed[jobs] {
	status[jobs] = "skipped"
}

END {
	printf "{\n  \"run\": %s,\n  \"jobs\": [\n", json_string(ENVIRON["RUN"]) > OUT
	passed = 0
	for (i = 1; i <= jobs; i++)
		passed += status[i] == "passed"
	printf "%s: %d/%d passed\n", ENVIRON["DIR"], passed, jobs
	for (i = 1; i <= jobs; i++) {
		printf "    {\n      \"name\": %s,\n", json_string(name[i]) > OUT
		printf "      \"status\": \"%s\",\n", status[i] > OUT
		printf "      \"failure\": %s,\n", (failed[i] ? json_string(message[i]) : "null") > OUT
		printf "      \"elapsed_seconds\": %s,\n", (elapsed[i] == "" ? "0" : elapsed[i]) > OUT
		printf "      \"resources\": [\n" > OUT
		write_resources(i)
		printf "      ]\n    }%s\n", (i < jobs ? "," : "") > OUT
		printf "%s %ss %s peak_process_rss=%sMiB max_growth=%sMiB\n", status[i], elapsed[i], name[i], job_peak, (job_growth == "" ? "n/a" : job_growth)
	}
	printf "  ]\n}\n" > OUT
	close(OUT)
}
'

for dir; do
	summarize "$dir"
done
