-- SocialPulse: analytical queries answering the six business questions.
-- engagement = likes + comments + shares   (views are impressions, tracked separately)

-- Q1. Total engagement per post (top 20)
SELECT p.post_id, pl.platform_name, p.content_type,
       f.likes, f.comments, f.shares, f.total_engagement, f.impressions
FROM fact_post_engagement f
JOIN dim_post p      ON p.post_key = f.post_key
JOIN dim_platform pl ON pl.platform_key = f.platform_key
ORDER BY f.total_engagement DESC
LIMIT 20;

-- Q2. Engagement rate per creator: (likes+comments+shares) / followers x 100, latest day
SELECT c.username, c.niche, c.city, d.follower_count, d.total_engagement,
       round(d.total_engagement::NUMERIC / NULLIF(d.follower_count, 0) * 100, 4) AS engagement_rate_pct
FROM fact_creator_daily d
JOIN dim_creator c ON c.creator_key = d.creator_key
WHERE d.date_key = (SELECT max(date_key) FROM fact_creator_daily)
  AND d.posts_count > 0
ORDER BY engagement_rate_pct DESC NULLS LAST
LIMIT 20;

-- Q3. Follower growth rate: (current - previous) / previous x 100, fastest-growing creators
SELECT c.username, c.niche, s.previous_follower_count, s.follower_count,
       round((s.follower_count - s.previous_follower_count)::NUMERIC
             / NULLIF(s.previous_follower_count, 0) * 100, 4) AS growth_rate_pct
FROM fact_follower_snapshots s
JOIN dim_creator c ON c.creator_key = s.creator_key
WHERE s.date_key = (SELECT max(date_key) FROM fact_follower_snapshots)
  AND s.previous_follower_count IS NOT NULL
ORDER BY growth_rate_pct DESC
LIMIT 20;

-- Q4. Engagement by platform
SELECT pl.platform_name,
       count(*)                       AS posts,
       sum(f.total_engagement)        AS total_engagement,
       sum(f.impressions)             AS impressions,
       round(avg(f.total_engagement), 2) AS avg_engagement_per_post
FROM fact_post_engagement f
JOIN dim_platform pl ON pl.platform_key = f.platform_key
GROUP BY pl.platform_name
ORDER BY total_engagement DESC;

-- Q5. Engagement by hashtag (min. 5 posts so one lucky post does not dominate)
SELECT hashtag,
       sum(posts_used)       AS posts_used,
       sum(total_engagement) AS total_engagement,
       round(sum(total_engagement)::NUMERIC / sum(posts_used), 2) AS avg_engagement_per_post
FROM fact_hashtag_performance
GROUP BY hashtag
HAVING sum(posts_used) >= 5
ORDER BY avg_engagement_per_post DESC
LIMIT 20;

-- Q6a. Best posting hour (Nairobi time)
SELECT p.posted_hour_eat AS hour_eat, count(*) AS posts,
       round(avg(f.total_engagement), 2) AS avg_engagement_per_post
FROM fact_post_engagement f
JOIN dim_post p ON p.post_key = f.post_key
GROUP BY p.posted_hour_eat
ORDER BY avg_engagement_per_post DESC;

-- Q6b. Best posting day of week
SELECT p.posted_day_of_week AS iso_dow, p.posted_day_name AS day_name, count(*) AS posts,
       round(avg(f.total_engagement), 2) AS avg_engagement_per_post
FROM fact_post_engagement f
JOIN dim_post p ON p.post_key = f.post_key
GROUP BY p.posted_day_of_week, p.posted_day_name
ORDER BY p.posted_day_of_week;

-- Bonus: creator history (SCD2) - who changed city/niche/username and when
SELECT user_id, username, niche, city, valid_from, valid_to, is_current
FROM dim_creator
WHERE user_id IN (SELECT user_id FROM dim_creator GROUP BY user_id HAVING count(*) > 1)
ORDER BY user_id, valid_from;
