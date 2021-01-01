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

-- column_check possibly only alters target table to add columns,
--   which, as a DDL statement, is executed in its own transaction
-- Note: automigrate is hardcoded to true here on purpose
{{if eq .stage_next true}}

CALL {{.output_schema}}.column_check(
    UPPER('{{.scratch_schema}}'),
    UPPER('events_this_run{{.entropy}}'),
    UPPER('{{.scratch_schema}}'),
    UPPER('events_staged{{.entropy}}'),
    'TRUE'
);

{{end}}

-- commit_with_metadata acceps stage_next as a variable
-- As there are no production/derived tables for base module,
--   it is the same as setting skip_derived to true.
CALL {{.output_schema}}.commit_with_metadata(
    UPPER('{{.output_schema}}.datamodel_metadata{{.entropy}}'),      -- metadata
    UPPER('{{.scratch_schema}}.base_metadata_this_run{{.entropy}}'), -- metadata from

    UPPER('{{.stage_next}}'),                -- if stage_next
    UPPER('{{.scratch_schema}}'),            -- source schema
    UPPER('events_this_run{{.entropy}}'),    -- source table
    UPPER('{{.scratch_schema}}'),            -- target schema
    UPPER('events_staged{{.entropy}}'),      -- target table
    UPPER('event_id'),                       -- staging join key

    'TRUE',                                  -- skip_derived harcoded to true
    '','','','','','',                       -- other skip_derived related args
    0                                        -- dummy argument
);
