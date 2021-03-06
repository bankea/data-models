#!/bin/bash

# Expected input:
# bash run_playbooks.sh {path_to_sql_runner} {database} {major version} '{list_of_playbooks_no_extension},{comma_separated}' {credentials (optional)}

root_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )/..
script_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

echo $root_path

# Create temp dir if not exists
mkdir -p $root_path/tmp

#Always remove files which may contain creds on exit
set -e
cleanup() {
  echo "run_playbooks: Removing /tmp/ files"
  rm  -f $root_path/tmp/current_playbook.yml
}
trap cleanup EXIT


if [ "$2" == "redshift" ]; then

  PASSWORD=${DB_PASSWORD:-$5}
  TEMPLATE=$script_path/templates/redshift_template.yml.tmpl
  MODEL_PATH=$root_path/web/$3/redshift/sql-runner
  # TODO: Fix path here


elif [ "$2" == "bigquery" ]; then
  BIGQUERY_CREDS=${BIGQUERY_CREDS:-$5}
  TEMPLATE=$script_path/templates/bq_template.yml.tmpl
  MODEL_PATH=$root_path/web/$3/bigquery/sql-runner
  export GOOGLE_APPLICATION_CREDENTIALS=$root_path/tmp/bq_creds.json

  if [ -n "$BIGQUERY_CREDS" ]; then

    cleanup() {
      echo "run_playbooks: Removing playbook file"
      rm -f $root_path/tmp/current_playbook.yml
      echo "run_playbooks: Removing credentials file"
      rm -f $root_path/tmp/bq_creds.json
    }

    echo "run_playbooks: writing bq creds to file"
    echo $BIGQUERY_CREDS > $root_path/tmp/bq_creds.json

  fi

# Yet to be implemented:
elif [ "$2" == "snowflake" ]; then
  echo "run_playbooks: snowflake v1 not implemented yet. Use the old scripts to run v0.9.X"
fi

# Interpret comma separated list of playbooks
IFS=',' read -ra pbk_array <<< "$4"

# Run each playbook
for i in "${pbk_array[@]}";
do

  # Append template and playbook, subbing in credentials:
  sed "s/PASSWORD_PLACEHOLDER/$PASSWORD/" $TEMPLATE > $root_path/tmp/current_playbook.yml
  sed "1,/^:variables:$/d" $MODEL_PATH/playbooks/$i.yml.tmpl >> $root_path/tmp/current_playbook.yml

  echo "run_playbooks: starting playbook: $i"

  # Create run command
  run_command='$1 -playbook $root_path/tmp/current_playbook.yml -sqlroot $MODEL_PATH/sql'

  eval $run_command

  echo "run_playbooks: done with playbook: $i";
done;
