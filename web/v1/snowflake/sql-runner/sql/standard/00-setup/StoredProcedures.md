# Stored procedures

Below you can read about the stored procedured defined for the Snowflake modules, that you can also use in your custom modules as well. They are defined under the `{{.output_schema}}` and you can find the source code [here](./01-main/01-stored-procedures.sql).

For modularity, they are recreated in the first step of all `01-main` playbooks and they are explicitly dropped in the `XX-destroy` steps.

Snowflake's [stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures.html) are written in JavaScript and provide the ability to execute SQL through a JavaScript API. This makes it possible to leverage JavaScript and introduce complex procedural and error-handling logic or create SQL statements dynamically.


## mk\_transaction

This is a stored procedure created to group DML statements into an atomic transaction. It mainly addresses what Snowflake docs describe [here](https://docs.snowflake.com/en/sql-reference/transactions.html#failed-statements-within-a-transaction) as:

> Although a transaction is committed or rolled back as a unit, that is not quite the same as saying that it succeeds or fails as a unit. If a statement fails within a transaction, you can still commit, rather than roll back, the transaction.


The `mk_transaction` procedure can be used when you want to ensure that either all the statements in the block succeed and get committed or all get rolled back.

**Argument**

 - A concatenation of DML statements separated by semicolon.

**Notes**

1. It is important that the statements are not DDL. DDL statements (e.g. `CREATE TABLE`) execute on their own transaction, so including them essentially "breaks" the abortability.
2. The DML statements inside the string argument are expected to be separated by semicolon and comments are not handled.

**Example call**

```
CALL derived.mk_transaction(
 '
 DELETE FROM A_TBL;
 INSERT INTO A_TBL VALUES (1,2),(3,4);
 TRUNCATE TABLE B_TBL;
 '
);
```

## column\_check

This stored procedure can be used to check for mismatched columns between source and target tables and potentially automigrate any mismatch of target.

**Arguments**

 - source schema (varchar)
 - source table name (varchar)
 - target schema (varchar)
 - target table name (varchar)
 - automigrate flag (boolean)

If automigrate is enabled it alters the target table to get added the additional columns,if any, in source. In any case, if the target table's columns are not a subset of source's, it errors.

**Notes**

1. Automigration retains the column type (including character maximum length for VARCHAR and scale and precision for NUMBER).
2. Automigration only migrates the columns. It does not transfer constraints and it does not insert any data to the target table. So, existing rows in target will get NULL as value for any new columns.

**Example call**

```SQL
-- Assuming tables A_TBL with columns c1,c2,c3 and B_TBL with columns c2
-- this will add columns c1 and c3 to B_TBL
CALL derived.column_check('A_SCHEMA',
                          'A_TBL',
                          'B_SCHEMA',
                          'B_TBL',
                          'TRUE');
```


## commit\_with\_metadata

This stored procedure is being used in the commit steps of the standard modules. It groups together:

1. Committing metadata
2. Inserts and deletes(in transaction) for staged tables, if `stage_next` is set to true
3. Inserts and deletes(in transaction) to production tables, if `skip_derived` is set to false

**Arguments**

 - production metadata table
 - `_this_run`metadata table
 - `stage_next` flag (as varchar)
 - source schema (for staging)
 - source table name (for staging)
 - target schema
 - target table name
 - join key for staged table
 - `skip_derived` flag (as varchar)
 - source schema
 - source table name
 - target schema
 - target table name
 - join key for derived table
 - partition key for derived table
 - `upsert_lookback_days` (as float)

**Example call**

```
CALL commit_with_metadata(UPPER('derived.datamodel_metadata'),
                          UPPER('scratch.pv_metadata_this_run'),
                          'TRUE',
                          UPPER('scratch'),
                          UPPER('page_views_this_run'),
                          UPPER('scratch'),
                          UPPER('page_views_staged'),
                          UPPER('page_view_id'),
                          'FALSE',
                          UPPER('scratch'),
                          UPPER('page_views_this_run'),
                          UPPER('derived'),
                          UPPER('page_views'),
                          UPPER('page_view_id'),
                          UPPER('start_tstamp'),
                          30::FLOAT);
```

## ctas\_similar

This procedure creates the target table as "similar" to the source table. It imitates BigQuery's `EXCEPT(col1,...)` functionality and also allows the creation of a target table with replacing columns with their (simple) transformation.

For example, in the standard model we use it to replace the `contexts_com_snowplowanalytics_snowplow_web_page_1` column, which is of `variant` type, with the value of its key `id` (casted to `varchar`) and alias it as `page_view_id`.

**Arguments**

 - source schema
 - source table name
 - comma-separated concatenation of columns to exclude
 - comma-separated concatenation of simple column transformation
 - comma-separated concatenation of new column aliases
 - target schema
 - target table name

For example, if someone wanted to create or replace a table similar to `base_events_this_run` but
 - excluding the columns `br_lang` and `geo_latitude`
 - exclude the yauaa context and replace it with 2 columns: its `operatingSystemClass` as `yauaa_os_class`, and its `operatingSystemName` as `yauaa_os_name`
 - similarly replacing the `link_click` unstructured event with the `targetUrl` as `target_url`

then:

**Example call**

```
CALL ctas_similar(
    UPPER('scratch'),
    UPPER('base_events_this_run'),
    UPPER('br_lang,geo_latitude, contexts_nl_basjes_yauaa_context_1,unstruct_event_com_snowplowanalytics_snowplow_link_click_1'),
    UPPER('contexts_nl_basjes_yauaa_context_1[0]:operatingSystemClass::varchar, contexts_nl_basjes_yauaa_context_1[0]:operatingSystemName::varchar,unstruct_event_com_snowplowanalytics_snowplow_link_click_1:targetUrl::varchar'),
    UPPER('yauaa_os_class,yauaa_os_name,target_url'),
    UPPER('scratch'),
    UPPER('my_new_table'));
```

## Troubleshooting

When troubleshooting, you can also consider:
 - The case sensitivity of Snowflake (you will notice that in our calls we use `UPPER` for varchar arguments). You can read more about it [here](https://docs.snowflake.com/en/sql-reference/stored-procedures-usage.html#case-sensitivity-in-javascript-arguments).
 - The flags are of `VARCHAR` type and not of `BOOLEAN`.
 - Besides the order, the number of arguments matters. Stored procedure names can be overloaded.

Also, feel free to reach out!
