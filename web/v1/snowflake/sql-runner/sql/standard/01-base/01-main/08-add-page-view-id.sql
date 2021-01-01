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
  Does same as what BigQuery does with:

  SELECT
    a.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[SAFE_OFFSET(0)].id AS page_view_id,
    a.* EXCEPT(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0)
  FROM a
*/
CALL {{.output_schema}}.ctas_similar(
    UPPER('{{.scratch_schema}}'),                                        -- source schema
    UPPER('base_events_this_run_tmp{{.entropy}}'),                       -- source table
    UPPER('contexts_com_snowplowanalytics_snowplow_web_page_1'),         -- exclude col
    'contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar(36)', -- new col
    'page_view_id',                                                          -- alias
    UPPER('{{.scratch_schema}}'),                                        -- target schema
    UPPER('events_this_run{{.entropy}}')                                 -- target table
);
