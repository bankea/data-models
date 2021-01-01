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


CREATE OR REPLACE TABLE {{.scratch_schema}}.page_views_join_staged{{.entropy}}
AS (
    WITH link_clicks AS (
        SELECT
            ev.page_view_id,

            COUNT(ev.event_id) OVER (
              PARTITION BY ev.page_view_id
              ORDER BY ev.derived_tstamp DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            AS link_clicks,

            FIRST_VALUE(ev.unstruct_event_com_snowplowanalytics_snowplow_link_click_1:targetUrl::varchar) OVER (
              PARTITION BY ev.page_view_id
              ORDER BY ev.derived_tstamp DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            AS first_link_target

        FROM
            {{.scratch_schema}}.events_staged{{.entropy}} AS ev

        WHERE
            page_view_id IS NOT NULL

    ), engagement AS (
        SELECT
            page_view_id,
            start_tstamp,

            CASE
              WHEN engaged_time_in_s = 0 THEN TRUE
              ELSE FALSE
            END AS bounced_page_view,

            (vertical_percentage_scrolled / 100) * 0.3 + (engaged_time_in_s / 600) * 0.7 AS engagement_score  -- tbc

        FROM
            {{.scratch_schema}}.page_views_staged{{.entropy}}
    )

    SELECT
        ev.page_view_id,
        eng.start_tstamp,

        lc.link_clicks,
        lc.first_link_target,

        eng.bounced_page_view,
        eng.engagement_score,

        CASE
          WHEN ev.refr_medium = 'search'
           AND RLIKE(LOWER(ev.mkt_medium), '.*(cpc|ppc|sem|paidsearch).*')
            OR RLIKE(LOWER(ev.mkt_source), '.*(cpc|ppc|sem|paidsearch).*') THEN 'paidsearch'

          WHEN ILIKE(ev.mkt_medium, '%paidsearch%')
            OR ILIKE(ev.mkt_source, '%paidsearch%') THEN 'paidsearch'

          WHEN RLIKE(LOWER(mkt_source), '.*(adwords|google_paid|googleads).*')
            OR RLIKE(LOWER(mkt_medium), '.*(adwords|google_paid|googleads).*') THEN 'paidsearch'

          WHEN ILIKE(ev.mkt_source, '%google%')
           AND ILIKE(ev.mkt_medium, '%ads%') THEN 'paidsearch'

          WHEN ev.refr_urlhost IN ('www.googleadservices.com','googleads.g.doubleclick.net') THEN 'paidsearch'

          WHEN RLIKE(LOWER(ev.mkt_medium), '.*(cpv|cpa|cpp|content-text|advertising|ads).*') THEN 'advertising'

          WHEN RLIKE(LOWER(ev.mkt_medium), '.*(display|cpm|banner).*') THEN 'display'

          WHEN ev.refr_medium IS NULL
           AND NOT ILIKE(ev.page_url, '%utm_%') THEN 'direct'

          WHEN (LOWER(ev.refr_medium) = 'search' AND ev.mkt_medium IS NULL)
            OR (LOWER(ev.refr_medium) = 'search' AND LOWER(ev.mkt_medium) = 'organic') THEN 'organicsearch'

          WHEN ev.refr_medium = 'social'
            OR RLIKE(LOWER(ev.mkt_source), '^((.*(facebook|linkedin|instagram|insta|slideshare|social|tweet|twitter|youtube|lnkd|pinterest|googleplus|instagram|plus.google.com|quora|reddit|t.co|twitch|viadeo|xing|youtube).*)|(yt|fb|li))$')
            OR RLIKE(LOWER(ev.mkt_medium), '^.*(social|facebook|linkedin|twitter|instagram|tweet).*$') THEN 'social'

          WHEN ev.refr_medium = 'email'
            OR ILIKE(ev.mkt_medium, '_mail') THEN 'email'

          WHEN ILIKE(ev.mkt_medium, 'affiliate') THEN 'affiliate'

          WHEN ev.refr_medium = 'unknown'
            OR ILIKE(ev.mkt_medium, 'referral')
            OR ILIKE(ev.mkt_medium, 'referal') THEN 'referral'

          WHEN ev.refr_medium = 'internal' THEN 'internal'

          ELSE 'others'
        END AS channel

    FROM
        {{.scratch_schema}}.events_staged{{.entropy}} AS ev

    LEFT JOIN link_clicks AS lc
        ON lc.page_view_id = ev.page_view_id

    LEFT JOIN engagement AS eng
        ON eng.page_view_id = ev.page_view_id

    WHERE event_name = 'page_view'
);
