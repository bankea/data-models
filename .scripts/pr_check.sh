#!/bin/bash

# Expected input:
# -b (binary) path to sql-runner binary
# -d (database) target database for expectations
# -a (auth) optional credentials for database target

while getopts 'b:d:a:' v
do
  case $v in
    b) SQL_RUNNER_PATH=$OPTARG ;;
    d) DATABASE=$OPTARG ;;
    a) CREDENTIALS=$OPTARG ;;
  esac
done

script_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

echo "pr_check: Starting 10 e2e iterations"

for i in {1..10}; do
  echo "pr_check: Starting e2e run $i";

  bash $script_path/e2e.sh -b $SQL_RUNNER_PATH -d $DATABASE -a $CREDENTIALS || exit 1;

  echo "pr_check: e2e run $i Done";

done || exit 1

echo "pr_check: Done"
