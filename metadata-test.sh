#!/bin/bash
#===============================================================================
#
#          FILE: metadata-test.sh
# 
#         USAGE: See script_help() or --help
# 
#   DESCRIPTION: Tries to discover as manay PATHs as possible in the --timeout
#                period, then stat (in serial and parallel) all of the paths.
#                Reports time taken to do each. 
# 
#       OPTIONS: See script_help() or --help
#  REQUIREMENTS: GNU Utils
#          BUGS: See GitHub repository
#        AUTHOR: Raychel Benson-Cahoy (bens0352@umn.edu)
#  ORGANIZATION: MSI ASO-PSI
#       CREATED: 2023-02-07
#      REVISION: v1.0
#===============================================================================

set -o nounset
set -o pipefail

function script_help() {
  echo "Usage: $0 --target DIR --time SECONDS [--repeat COUNT] [--log PATH] [--output csv|key-value]"
  echo ""
  echo "Run rudimentary user-space traversal and statting tests on"
  echo "a given DIR."
  echo "  1) Scan the target for 'paths' for --time # seconds (default 60)"
  echo "  3) time stat --terse, in parallel (24 threads), all paths found in (1)"
  echo "  2) time stat --terse, in serial, all the paths found in (1)"
  echo "Output the results to stdout or --log PATH if specified"
  echo ""
  echo "NOTE: Path discovery and stat rates are highly affected by filesystem"
  echo "      caching (espeically on network filesystems where the client will"
  echo "      add a second layer of cache). First runs will likely show the"
  echo "      'cold' rates.  Subsequent runs (--repeat) will likely show the 'warm' rates"
  echo ""
  echo "REQUIRED ARGUMENTS"
  echo " --target DIR    The directory to scan/test against"
  echo " --time SECONDS  The number of seconds to allow for the 'search' phase"
  echo "                 of the test.  (default: 60s). The search may end before"
  echo "                 this time if the filesystem paths are exaused first."
  echo ""
  echo "REPEAT OPTIONS"
  echo " --repeat COUNT  Run the test # of times"
  echo " --delay SECONDS Pause SECONDS between each --repeat (default: 0)"
  echo ""
  echo "OUTPUT OPTIONS"
  echo " --format FORMAT FORMAT can be 'csv' or 'key-value'.  Output or log results"
  echo "                 in the specified FORMAT. (default: key-value)"
  echo " --log FILE      Store the test results in FILE (default: /tmp/metadata-test.log)"
  # echo " --crunch        Output the min, max, and average of every field at the end"
}

function defaults() {
  search_time=60
  repeat_count=1
  delay_interval=0
  log="/tmp/metadata-test.log"
  format='key-value'
  # run_averages='false'
}

function arg_parser() {
  if [ $# -eq 0 ]; then
    script_help
    exit
  fi
  while [ $# -gt 0 ]; do
    case "$1" in
      --target)
        target="$2"
        shift 2
        ;;
      --time)
        search_time="$2"
        shift 2
        ;;
      --repeat)
        repeat_count="$2"
        shift 2
        ;;
      --delay)
        delay_interval="$2"
        shift 2
        ;;
      --format)
        if [ "$2" == 'csv' ] || [ "$2" == 'key-value' ]; then
          format="$2"
          shift 2
        else
          >&2 echo "Error: '$2' is not a valid FORMAT. See --help"
        fi
        ;;
      # --crunch|--calc)
      #   run_averages='true'
      #   shift
      #   ;;
      *)
        script_help
        exit
        ;;
    esac
  done
}

function run_tests() {
  # Prin the CSV header
  #printf 'SearchTime,ObjectsFound,SerialStatsPerSecond,ParallelStatsPerSecond\n'
  # printf 'timeStamp,pathsFound,searchTime,paths/s,serialStatTime,serialStats/s,parallelStatTime,parallelStats/s\n'

  # Set some local var handles
  local _test_start _test_end _parallel_time _parallel_rate _serial_time
  local _serial_rate _paths_time _paths_found _paths_rate _start_ts
  
  # Make a run counter for logging
  local _run_count=0
  while [ $_run_count -lt ${repeat_count:-1} ]; do

    # Only make the path's file if this is the first pass
    if [ ! -f "${_paths_file:-}" ]; then
      _paths_file="$(mktemp --tmpdir=/dev/shm -t metadata-test-tmp.XXXXXXXX)"
    fi

    _start_ts="$(date +%s)"
    # Run the path-search test
    _test_start="$(date +%s.%N)"
    timeout $search_time find -H "$target" -xdev -fprint0 "$_paths_file" 2>/dev/null
    _test_end="$(date +%s.%N)"
    _paths_time="$(<<< "$_test_end-$_test_start" bc)"
    _paths_found="$(tr '\0' '\n' <"$_paths_file" | wc -l)"
    _paths_rate="$(<<< "$_paths_found / $_paths_time" bc)"

    # Start the parallel stat test
    local _parallel_chunks="$(( _paths_found / 24 ))"
    _test_start="$(date +%s.%N)"
    >/dev/null 2>&1 xargs --null -a "$_paths_file" -P 24 -n $_parallel_chunks stat --terse
    _test_end="$(date +%s.%N)"
    _parallel_time="$(<<< "$_test_end-$_test_start" bc -l)"
    _parallel_rate="$(<<< "$_paths_found / $_parallel_time" bc -l)"

    # Start the serial stat test
    _test_start="$(date +%s.%N)"
    >/dev/null 2>&1 xargs --null -a "$_paths_file" stat --terse
    _test_end="$(date +%s.%N)"
    _serial_time="$(<<< "$_test_end-$_test_start" bc -l)"
    _serial_rate="$(<<< "$_paths_found / $_serial_time" bc -l)"

    # Output the test results in the indicated format
    case "$format" in
      'csv')
        printf '%s,%s,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f\n' "$_start_ts" "$_paths_found" \
        "$_paths_time" "$_paths_rate" "$_serial_time" "$_serial_rate" \
        "$_parallel_time" "$_parallel_rate"
      ;;
      'key-value')
        printf 'testTimestamp: %s\n' "$_start_ts" 
        printf 'pathsFound: %s\n' "$_paths_found"
        printf 'pathsTime: %.1f\n' "$_paths_time"
        printf 'pathsRate: %.1f\n' "$_paths_rate"
        printf 'serialStatTime: %.1f\n' "$_serial_time"
        printf 'serialStatRate: %.1f\n' "$_serial_rate"
        printf 'parallelStatTime: %.1f\n' "$_parallel_time"
        printf 'parallelStatRate: %.1f\n' "$_parallel_rate"
      ;;
    esac

    # Increment the run count
    ((_run_count=_run_count+1))

    # Sleep if there's a delay specified
    sleep "$delay_interval"
  done

  rm "$_paths_file"
}

# function calculate_averages() {
#   # Get the number of samples in the log
#   _samples="$(( $(wc -l "$log" | cut -d ' ' -f 1) - 1 ))"

#   # Calculate min/max/average searchTimes
#   _searchTime_avg="$(<<<"( $(cut -d ',' -f 3 "$log" | tail -n +2 | paste -sd+) ) / $_samples" bc -l)"
#   _searchTime_max="$(cut -d ',' -f 3 "$log" | tail -n +2 | sort -n | tail -n 1)"
#   _searchTime_min="$(cut -d ',' -f 3 "$log" | tail -n +2 | sort -n | head -n 1)"

#   # Number of discovered objects
#   _discovered_avg="$(<<<"( $(cut -d ',' -f 2 "$log" | tail -n +2 | paste -sd+) ) / $_samples" bc -l)"
#   _discovered_max="$(cut -d ',' -f 2 "$log" | tail -n +2 | sort -n | tail -n 1)"
#   _discovered_min="$(cut -d ',' -f 2 "$log" | tail -n +2 | sort -n | head -n 1)"

#   # serial stat rate
#   _serialStat_avg="$(<<<"( $(cut -d ',' -f 6 "$log" | tail -n +2 | paste -sd+) ) / $_samples" bc -l)"
#   _serialStat_max="$(cut -d ',' -f 6 "$log" | tail -n +2 | sort -n | tail -n 1)"
#   _serialStat_min="$(cut -d ',' -f 6 "$log" | tail -n +2 | sort -n | head -n 1)"

#   # parallel stat rate
#   _parallelStat_avg="$(<<<"( $(cut -d ',' -f 8 "$log" | tail -n +2 | paste -sd+) ) / $_samples" bc -l)"
#   _parallelStat_max="$(cut -d ',' -f 8 "$log" | tail -n +2 | sort -n | tail -n 1)"
#   _parallelStat_min="$(cut -d ',' -f 8 "$log" | tail -n +2 | sort -n | head -n 1)"
  
#   # Print the min, max, and average
#   printf '\n'
#   printf 'searchTime,pathsDiscovered,serialStats/s,parallelStats/s\n'
#   printf 'Min: %.3f,%.1f,%.3f,%.3f\n' "$_searchTime_min" "$_discovered_min" "$_serialStat_min" "$_parallelStat_min"
#   printf 'Max: %.3f,%.1f,%.3f,%.3f\n' "$_searchTime_max" "$_discovered_max" "$_serialStat_max" "$_parallelStat_max"
#   printf 'Avg: %.3f,%.1f,%.3f,%.3f\n' "$_searchTime_avg" "$_discovered_avg" "$_serialStat_avg" "$_parallelStat_avg"
# }

function main() {
  defaults
  arg_parser "$@"
  # Run the tests.  Sore the results in the given log file
  run_tests | tee "$log"
  # Calculate the averages from the log file and append them
  # if [ "$run_averages" = 'true' ]; then
  #   calculate_averages | tee -a "$log"
  # fi
}

main "$@"
