

CREATE SCHEMA fin_test;

CREATE TABLE fin_test.execution_timestamp (
	executiontime timestamp NULL
);


CREATE TABLE fin_test.youtube_videos (
	id serial4 NOT NULL,
	query varchar(255) NULL,
	video_id varchar(50) NULL,
	video_title text NULL,
	video_description text NULL,
	published_at timestamp NULL,
	channel_id varchar(50) NULL,
	view_count int4 NULL,
	like_count int4 NULL,
	comment_count int4 NULL,
	default_audio_language varchar(10) NULL,
	video_duration interval NULL,
	tags _text NULL
);


CREATE TABLE fin_test.youtube_channels (
	channel_id varchar(50) NOT NULL,
	channel_title varchar(255) NULL,
	channel_description text NULL,
	subscriber_count int4 NULL,
	video_count int4 NULL,
	published_at timestamp NULL
);

CREATE TABLE fin_test.dim_videos_details (
	id_videos serial4 NOT NULL,
	video_id varchar NOT NULL,
	video_title text NULL,
	video_description text NULL,
	published_at timestamp NULL,
	default_audio_language varchar NULL,
	video_duration interval NULL,
	tags _text NULL,
	CONSTRAINT dim_videos_details_un UNIQUE(video_id),
	CONSTRAINT dim_video_details_pkey PRIMARY KEY (id_videos)
);

CREATE TABLE fin_test.dim_channels (
	id_channels serial4 NOT NULL,
	channel_id varchar(50) NOT NULL,
	channel_title varchar(255) NULL,
	channel_description text NULL,
	subscriber_count int4 NULL,
	video_count int4 NULL,
	published_at timestamp NULL,
	CONSTRAINT dim_channels_un UNIQUE (channel_id),
	CONSTRAINT youtube_channels_pkey PRIMARY KEY (id_channels)
);


CREATE TABLE fin_test.fact_videos (
	id_videos integer REFERENCES dim_videos_details(id_videos) NOT NULL,
	id_channels integer REFERENCES dim_channels(id_channels) NOT NULL,
	video_id varchar NULL,
	channel_id varchar NULL,
	view_count int4 NULL,
	like_count int4 NULL,
	comment_count int4 NULL,
	"timestamp" timestamp NULL,
	query text NULL
);