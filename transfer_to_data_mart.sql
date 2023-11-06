INSERT INTO dim_channels  (channel_id, channel_title, channel_description, subscriber_count, video_count, published_at)
SELECT channel_id, channel_title, channel_description, subscriber_count, video_count, published_at
FROM fin_test.youtube_channels
ON CONFLICT (channel_id) DO UPDATE
SET 
    channel_title = EXCLUDED.channel_title,
    channel_description = EXCLUDED.channel_description,
    subscriber_count = EXCLUDED.subscriber_count,
    video_count = EXCLUDED.video_count,
    published_at = EXCLUDED.published_at;

   


INSERT INTO dim_videos_details (video_id, video_title, video_description, published_at, default_audio_language, video_duration, tags)
SELECT DISTINCT  yt.video_id, yt.video_title, yt.video_description, yt.published_at, yt.default_audio_language, yt.video_duration, yt.tags
FROM fin_test.youtube_videos yt
ON CONFLICT (video_id) DO UPDATE 
SET 
    video_title = EXCLUDED.video_title,
    video_description = EXCLUDED.video_description,
    published_at = EXCLUDED.published_at,
    default_audio_language = EXCLUDED.default_audio_language,
    video_duration = EXCLUDED.video_duration,
    tags = EXCLUDED.tags;

   
   
   
   
INSERT INTO fin_test.fact_videos (id_videos, id_channels, video_id, channel_id, view_count, like_count, comment_count, "timestamp", query)
SELECT
    dv.id_videos, 
    dc.id_channels, 
    yv.video_id, 
    yv.channel_id, 
    yv.view_count, 
    yv.like_count, 
    yv.comment_count, 
    et.executiontime,
    yv.query
FROM fin_test.youtube_videos yv
INNER JOIN fin_test.dim_videos_details dv ON dv.video_id = yv.video_id
INNER JOIN fin_test.dim_channels dc ON dc.channel_id = yv.channel_id
CROSS JOIN fin_test.execution_timestamp et;