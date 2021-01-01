/*
   Copyright 2021 Snowplow Analytics Ltd. All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


/*
  MK_TRANSACTION
  Side effects procedure.
  Input: a concatenation of one or more DML sql statements split by semicolon.
  It is important that the statements are not DDL.
  Either all the statements in the block succeed and get committed or all get rolled back.
  To drop:
  DROP PROCEDURE {{.output_schema}}.mk_transaction(VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.mk_transaction(DML_STATEMENTS VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  dmls = DML_STATEMENTS.split(';')
                       .filter(function(stmt) {return stmt.trim() !== '';})
                       .map(function(stmt) {return stmt.trim() + ';';});

  snowflake.createStatement({sqlText: `BEGIN;`}).execute();
  try {

      dmls.forEach(function(stmt) {
          snowflake.createStatement({sqlText: stmt}).execute();
      });
      snowflake.createStatement({sqlText: `COMMIT;`}).execute();

  } catch(ERROR) {
      snowflake.createStatement({sqlText: `ROLLBACK;`}).execute();

      // Snowflake error is not very helpful here
      var err_msg = "Transaction rolled back.Probably failed: " + DML_STATEMENTS + " :Error: ";
      throw Error(err_msg + ERROR);
  }

  return "ok. Statements in transaction succeeded."

  $$
;


/*
  COLUMN_CHECK
  Checks for mismatched columns between source and target tables.
  If source table is missing columns, it errors.
  If AUTOMIGRATE is 'TRUE',
    it allows the target table to get added the additional columns,if any, in source.
  Since ALTER TABLE is a DDL statement, it will be its own transaction.
  This means that even if it was placed inside another transaction,
    it would commit that, and then start another(implicit) for its own execution.
  So, also it cannot be explicitly rolled back.
  This is why column_check is not part of the commit_with_metadata procedure.

  Input:
  SRC_SCHEMA:  the schema of the source table
  SRC_TBL:     the source table name
  TRG_SCHEMA:  the schema of the target table
  TRG_TBL:     the target table name
  AUTOMIGRATE: whether any extra columns in source table will be added in target table (only 'TRUE' enables)

  To drop:
  DROP PROCEDURE {{.output_schema}}.column_check(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.column_check(SRC_SCHEMA  VARCHAR,
                                                            SRC_TBL     VARCHAR,
                                                            TRG_SCHEMA  VARCHAR,
                                                            TRG_TBL     VARCHAR,
                                                            AUTOMIGRATE VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  var delim = '~';
  var sourceColumns = list_cols_with_type(SRC_SCHEMA,SRC_TBL,delim).split(delim);
  var targetColumns = list_cols_with_type(TRG_SCHEMA,TRG_TBL,delim).split(delim);

  if (targetColumns.some(notIncludedIn(sourceColumns)) === true) {

      throw "ERROR: Source table is missing column(s) which exist in target table.";

  } else {

      var columnAdditions = sourceColumns.filter(notIncludedIn(targetColumns));

      if ( columnAdditions.length !== 0 ) {
          if ( AUTOMIGRATE !== 'TRUE' ) {
              throw "ERROR: Target table is missing column(s),but automigrate is not enabled.";
          } else {
              add_columns_to(TRG_SCHEMA, TRG_TBL, columnAdditions.join(','));
              return "ok.Columns added."
          }
      } else {
          return "ok. Columns match."
      }
  }

  // == Helpers ==

  function list_cols_with_type(sch,tbl,delimiter) {
      var stmt = `
          SELECT
              LISTAGG(
                  CASE
                    WHEN isc.data_type='TEXT'
                      THEN CONCAT(isc.column_name, ' VARCHAR(',isc.character_maximum_length, ')')
                    WHEN isc.data_type='NUMBER'
                      THEN CONCAT(isc.column_name, ' NUMBER(', isc.numeric_precision, ',',isc.numeric_scale, ')')
                    ELSE
                      CONCAT(isc.column_name, ' ', isc.data_type)
                  END, '` + delimiter + `')
              WITHIN GROUP (order by isc.ordinal_position)
          FROM information_schema.columns AS isc
          WHERE table_schema='` + sch + `'
            AND table_name='` + tbl + `';`;

      var res = snowflake.createStatement({sqlText: stmt}).execute();
      res.next();
      result = res.getColumnValue(1);

      return result;
  }

  function notIncludedIn(arr) {
      return function(elt) {
        return ! arr.includes(elt);
      };
  }

  function add_columns_to(sch, tbl, cols) {
      var alter_stmt = `ALTER TABLE ` + sch + `.` + tbl + ` ADD COLUMN ` + cols;
      snowflake.createStatement({sqlText: alter_stmt}).execute();

      return "ok. Columns added.";
  }

  $$
;


/*
  COMMIT_WITH_METADATA
  Commits metadata and makes the necessary deletes/inserts for the commit steps in modules.
  Wraps the above in a transaction.
  Inputs:
  META_TBL:                  the metadata table to commit (datamodel_metadata)
  META_FROM_TBL:             the table from which to insert to metadata
  STAGE_NXT:                 corresponds to '{{.stage_next}}'
  STG_SRC_SCHEMA:            the schema for table as source
  STG_SRC_TBL:               the source table name (to stage)
  STG_TRG_SCHEMA:            the schema for table as target
  STG_TRG_TBL:               the target table name (stage)
  STG_JOIN_KEY:              the join key for staged tables
  SKIP_DRV:                  corresponds to '{{.skip_derived}}'
  DRV_SRC_SCHEMA:            the schema for table as source
  DRV_SRC_TBL:               the source table name (to derive)
  DRV_TRG_SCHEMA:            the schema for table as target
  DRV_TRG_TBL:               the target table name (derive)
  DRV_JOIN_KEY:              the join key for derived table
  DRV_PARTITION_KEY:         the partition key for derived table
  DRV_UPSERT_LOOKBACK FLOAT: corresponds to {{.upsert_lookback_days}}::float

  To drop:
  DROP PROCEDURE {{.output_schema}}.commit_with_metadata(VARCHAR,VARCHAR,VARCHAR,VARCHAR,
                                                         VARCHAR,VARCHAR,VARCHAR,VARCHAR,
                                                         VARCHAR,VARCHAR,VARCHAR,VARCHAR,
                                                         VARCHAR,VARCHAR,VARCHAR,FLOAT);

*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.commit_with_metadata(
                                                        META_TBL            VARCHAR,
                                                        META_FROM_TBL       VARCHAR,
                                                        STAGE_NXT           VARCHAR,
                                                        STG_SRC_SCHEMA      VARCHAR,
                                                        STG_SRC_TBL         VARCHAR,
                                                        STG_TRG_SCHEMA      VARCHAR,
                                                        STG_TRG_TBL         VARCHAR,
                                                        STG_JOIN_KEY        VARCHAR,
                                                        SKIP_DRV            VARCHAR,
                                                        DRV_SRC_SCHEMA      VARCHAR,
                                                        DRV_SRC_TBL         VARCHAR,
                                                        DRV_TRG_SCHEMA      VARCHAR,
                                                        DRV_TRG_TBL         VARCHAR,
                                                        DRV_JOIN_KEY        VARCHAR,
                                                        DRV_PARTITION_KEY   VARCHAR,
                                                        DRV_UPSERT_LOOKBACK FLOAT)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  // Columns hardcoded for ease.
  var meta_stmt = `INSERT INTO ` + META_TBL + `
                     SELECT
                       run_id,
                       model_version,
                       model,
                       module,
                       run_start_tstamp,
                       CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS run_end_tstamp,
                       rows_this_run,
                       distinct_key,
                       distinct_key_count,
                       time_key,
                       min_time_key,
                       max_time_key,
                       duplicate_rows_removed,
                       distinct_keys_removed
                     FROM
                       ` + META_FROM_TBL + `;`;

  if (SKIP_DRV !== 'TRUE') {
      var drv_trg_columns = list_cols(DRV_TRG_SCHEMA,DRV_TRG_TBL);
      var drv_src_columns = list_cols(DRV_SRC_SCHEMA,DRV_SRC_TBL);
      if (cols_check(drv_trg_columns, drv_src_columns) === false) {
          throw Error("ERROR: column mismatch:" + DRV_TRG_TBL + "," + DRV_SRC_TBL);
      }

      var drv_trg_ref = `` + DRV_TRG_SCHEMA + `.` + DRV_TRG_TBL;
      var drv_src_ref = `` + DRV_SRC_SCHEMA + `.` + DRV_SRC_TBL;
      var drv_join_condition = `` + DRV_JOIN_KEY + ` IN
                               (SELECT ` + DRV_JOIN_KEY + ` FROM ` + drv_src_ref + `)`;
      var drv_lower_limit = `
          SELECT
            TIMEADD(DAY, -` + DRV_UPSERT_LOOKBACK + `, MIN(` + DRV_PARTITION_KEY + `))
          FROM ` + drv_src_ref;

      var drv_delete_stmt = `
          DELETE FROM ` + drv_trg_ref + `
          WHERE ` + drv_join_condition + `
            AND ` + drv_trg_ref + `.` + DRV_PARTITION_KEY + `>=(` + drv_lower_limit + `);`;

      var drv_insert_stmt = `
          INSERT INTO ` + drv_trg_ref + `
            SELECT ` + drv_trg_columns + `
            FROM ` + drv_src_ref + `;`;
  }

  if (STAGE_NXT === 'TRUE') {
      var stg_trg_columns = list_cols(STG_TRG_SCHEMA,STG_TRG_TBL);
      var stg_src_columns = list_cols(STG_SRC_SCHEMA,STG_SRC_TBL);
      if (cols_check(stg_trg_columns, stg_src_columns) === false) {
          throw Error("ERROR: column mismatch:" + STG_TRG_TBL + "," + STG_SRC_TBL);
      }

      var stg_trg_ref = `` + STG_TRG_SCHEMA + `.` + STG_TRG_TBL;
      var stg_src_ref = `` + STG_SRC_SCHEMA + `.` + STG_SRC_TBL;
      var stg_del_condition = `` + STG_JOIN_KEY + ` IN
                              (SELECT ` + STG_JOIN_KEY + ` FROM ` + stg_src_ref + `)`;

      var stg_delete_stmt = `
          DELETE FROM ` + stg_trg_ref + `
          WHERE ` + stg_del_condition + `;`;

      var stg_insert_stmt = `
          INSERT INTO ` + stg_trg_ref + `
            SELECT ` + stg_trg_columns + `
            FROM ` + stg_src_ref + `;`;
  }

  // BEGIN TRANSACTION
  snowflake.createStatement({sqlText: `BEGIN;`}).execute();
  try {

      if (SKIP_DRV !== 'TRUE') {
          snowflake.createStatement({sqlText: drv_delete_stmt}).execute();
          snowflake.createStatement({sqlText: drv_insert_stmt}).execute();
      }
      if (STAGE_NXT === 'TRUE') {
          snowflake.createStatement({sqlText: stg_delete_stmt}).execute();
          snowflake.createStatement({sqlText: stg_insert_stmt}).execute();
      }
      snowflake.createStatement({sqlText: meta_stmt}).execute();

      snowflake.createStatement({sqlText: `COMMIT;`}).execute();

  } catch(ERROR) {

      snowflake.createStatement({sqlText: `ROLLBACK;`}).execute();
      throw ERROR;

  }
  return "ok. commit_with_metadata succeeded.";

  // == Helpers ==

  function list_cols(sch,tbl) {
      var stmt = `
          SELECT listagg(isc.column_name, ',') WITHIN GROUP (order by isc.ordinal_position)
          FROM information_schema.columns AS isc
          WHERE table_schema='` + sch + `'
            AND table_name='` + tbl + `';`;

      var res = snowflake.createStatement({sqlText: stmt}).execute();
      res.next();
      result = res.getColumnValue(1);

      return result;
  }

  function cols_check(colsStr1, colsStr2) {
      colsArr1 = colsStr1.split(',');
      colsArr2 = colsStr2.split(',');

      // since we know there are no duplicate cols in a table,just set comparison
      return colsArr1.every(function(c) {return colsArr2.includes(c);}) &&
             colsArr1.length === colsArr2.length;
  }

  $$
;


/*
  CTAS_SIMILAR
  This procedure creates the target table as "similar" to the source table.
   - Imitates BigQuery's EXCEPT(col1,...) functionality.
   - Accepts a string of comma separated column tranformations to add
   - and another with their respective aliases.

  Input:
  SRC_SCHEMA:    the schema of the source table
  SRC_TBL:       the source table name
  XCLUDE_COLS:   a string of comma-separated columns to exclude
  TRANS_COLS:    a string of comma-separated column tranformations
  TRANS_ALIASES: a string of comma-separated aliases for the corresponding trans_cols
  TRG_SCHEMA:    the schema of the target table
  TRG_TBL:       the target table name

  Example call:
  CALL ctas_similar(UPPER('scratch'),
                    UPPER('base_events_this_run'),
                    UPPER('contexts_com_snowplowanalytics_snowplow_web_page_1, my_unwanted_column'),
                    'contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar(36)',
                    'page_view_id',
                    UPPER('scratch'),
                    UPPER('events_this_run);

  To drop
  DROP PROCEDURE {{.output_schema}}.ctas_similar(VARCHAR,VARCHAR,VARCHAR,VARCHAR,
                                                 VARCHAR,VARCHAR,VARCHAR);

*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.ctas_similar(SRC_SCHEMA    VARCHAR,
                                                            SRC_TBL       VARCHAR,
                                                            XCLUDE_COLS   VARCHAR,
                                                            TRANS_COLS    VARCHAR,
                                                            TRANS_ALIASES VARCHAR,
                                                            TRG_SCHEMA    VARCHAR,
                                                            TRG_TBL       VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  var x_cols = str_to_arr(XCLUDE_COLS);
  var x_colsready = x_cols.join('\',\'');
  var t_cols = str_to_arr(TRANS_COLS);
  var t_alias = str_to_arr(TRANS_ALIASES);

  var sql_stmt = `
      SELECT listagg(isc.column_name, ',') WITHIN GROUP (order by isc.ordinal_position)
      FROM information_schema.columns AS isc
      WHERE table_schema='` + SRC_SCHEMA + `'
        AND table_name='` + SRC_TBL + `'
        AND column_name NOT IN ('` + x_colsready + `');`;

  var res = snowflake.createStatement({sqlText: sql_stmt}).execute();
  res.next();
  var result = res.getColumnValue(1);

  if (t_cols.length !== t_alias.length) {
      throw new Error("Please provide equal number of aliases to the transformed columns.");
  }

  if (t_cols.length > 0) {
      var new_cols = t_cols.map(function(val,idx) {return val + " AS " + t_alias[idx];})
                           .join(',');
      if (result !== '') {
          new_cols = new_cols + ',';
      }
  } else {
      if (result !== '') {
          var new_cols = '';
      } else {
          // for a more helpful error
          throw new Error("No columns selected for the target table.");
      }
  }

  var fin_query=`CREATE OR REPLACE TABLE ` + TRG_SCHEMA + `.` + TRG_TBL + `
                 AS
                 SELECT ` + new_cols
                          + result + `
                 FROM ` + SRC_SCHEMA + `.` + SRC_TBL;

  var fin_result = snowflake.createStatement({sqlText: fin_query}).execute();

  return 'ok. create_similar succeeded.';

  // == Helpers ==
  function str_to_arr(str) {
      return str.split(',')
                .map(function(c) {return c.trim();})
                .filter(function(c) {return c !== '';});
  }

  $$
;
