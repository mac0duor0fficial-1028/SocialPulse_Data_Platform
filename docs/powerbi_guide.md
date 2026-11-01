# Power BI dashboard guide

Power BI files (`.pbix`) are binary and cannot be generated from code, so this guide gives you the exact
steps, model, DAX and layout. Budget ~half a day. Export a screenshot to `docs/images/dashboard.png` for the README.

## 1. Connect
Home → **Get data → PostgreSQL database** → Server `localhost:5432`, Database `socialpulse`, **Import** mode.
Select: `dim_creator`, `dim_post`, `dim_platform`, `dim_date`, `dim_location`, `fact_post_engagement`,
`fact_follower_snapshots`, `fact_creator_daily`, `fact_hashtag_performance`.

## 2. Model (Model view)
Mark `dim_date` as the date table (`full_date`). Relationships (all many-to-one, single direction, dim → fact):

| From (fact) | To (dim) |
|---|---|
| fact_post_engagement[post_key] | dim_post[post_key] |
| fact_post_engagement[creator_key] | dim_creator[creator_key] |
| fact_post_engagement[platform_key] | dim_platform[platform_key] |
| fact_post_engagement[date_key] | dim_date[date_key] |
| fact_follower_snapshots[creator_key / date_key] | dim_creator / dim_date |
| fact_creator_daily[creator_key / date_key] | dim_creator / dim_date |
| fact_hashtag_performance[platform_key / date_key] | dim_platform / dim_date |
| dim_creator[location_key] | dim_location[location_key] |

Filter `dim_creator[is_current] = TRUE` only on pages that show *current* attributes (e.g. a creator list);
fact-linked visuals should keep all versions so history stays correct.

## 3. Measures (create in a `_Measures` table)
```DAX
Total Creators      = DISTINCTCOUNT ( dim_creator[user_id] )
Total Posts         = COUNTROWS ( fact_post_engagement )
Total Engagements   = SUM ( fact_post_engagement[total_engagement] )
Total Impressions   = SUM ( fact_post_engagement[impressions] )

Latest Followers =
VAR d = MAX ( fact_follower_snapshots[date_key] )
RETURN CALCULATE ( SUM ( fact_follower_snapshots[follower_count] ), fact_follower_snapshots[date_key] = d )

Avg Engagement Rate % =
AVERAGEX (
    FILTER ( VALUES ( dim_creator[creator_key] ), CALCULATE ( SUM ( fact_creator_daily[posts_count] ) ) > 0 ),
    CALCULATE ( AVERAGE ( fact_creator_daily[engagement_rate_pct] ) )
)

Follower Growth % =
VAR cur  = SUM ( fact_follower_snapshots[follower_count] )
VAR prev = SUM ( fact_follower_snapshots[previous_follower_count] )
RETURN DIVIDE ( cur - prev, prev ) * 100

Avg Engagement per Post = DIVIDE ( [Total Engagements], [Total Posts] )
Hashtag Posts           = SUM ( fact_hashtag_performance[posts_used] )
Hashtag Avg Engagement  = DIVIDE ( SUM ( fact_hashtag_performance[total_engagement] ), [Hashtag Posts] )
```

## 4. Pages
| Page | Visuals |
|---|---|
| **Overview** | Cards: Total Creators, Total Posts, Total Engagements, Avg Engagement Rate %. Line: engagements by `dim_date[full_date]`. Donut: engagements by platform |
| **Content performance** | Top-10 posts table (post_id, platform, content_type, total_engagement). Bar: Avg Engagement per Post by `dim_post[content_type]`. Heat-map matrix: `dim_post[posted_day_name]` × `dim_post[posted_hour_eat]` |
| **Audience analytics** | Line: Latest Followers over time by creator (top-N filter). Map/bar: engagements by `dim_location[city]`. Scatter: followers vs engagement rate per creator |
| **Hashtag analytics** | Bar: Hashtag Posts (most used, top 15). Bar: Hashtag Avg Engagement (min 5 posts via visual filter on Hashtag Posts ≥ 5) |

Slicers (sync across pages): platform, niche (`dim_creator`), city (`dim_location`), date range (`dim_date`).
**Drill-through** page "Creator detail": drill field `dim_creator[username]`; show follower curve, posts table, engagement rate trend.

## 5. Refresh
Desktop: Home → Refresh after each pipeline run. For scheduled refresh in the Power BI Service you need an on-premises data
gateway pointing at the PostgreSQL host.
