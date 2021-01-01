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


-- Since all columns are to be explicitly defined in model, there is no need
--   to call the column_check procedure (with automigrate hardcoded to false).
-- A simple check of columns is also done in commit_with_metadata, just in case.
CALL {{.output_schema}}.commit_with_metadata(
    UPPER('{{.output_schema}}.datamodel_metadata{{.entropy}}'),        -- metadata
    UPPER('{{.scratch_schema}}.users_metadata_this_run{{.entropy}}'),  -- metadata

    'FALSE',                                   -- if stage_next
    '','','','','',                            -- staging vars

    UPPER('{{.skip_derived}}'),                -- skip_derived
    UPPER('{{.scratch_schema}}'),              -- source schema
    UPPER('users_this_run{{.entropy}}'),       -- source table
    UPPER('{{.output_schema}}'),               -- target schema
    UPPER('users{{.entropy}}'),                -- target table
    UPPER('domain_userid'),                    -- join key
    UPPER('start_tstamp'),                     -- partition key
    0                                          -- upsert_lookback_days
);
