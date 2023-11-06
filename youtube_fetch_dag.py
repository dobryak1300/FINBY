from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

postgres_conn_id = 'postgres'

class YouTubeDataFetcherOperator(BaseOperator):

    def __init__(self, api_key, dbname, user, password, host, port, queries, *args, **kwargs):
        super(YouTubeDataFetcherOperator, self).__init__(*args, **kwargs)
        self.api_key = api_key
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.queries = queries
        self.channel_subscribers = {}

    def execute(self,context):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.conn.cursor()
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self.truncate_tables()
        for query in self.queries:
            self.fetch_data(query)
        self.update_execution_timestamp()
        self.close_connection()

    def fetch_data(self, query):

        last_execution_date = self.get_last_execution_date()

        search_request = self.youtube.search().list(
            part='snippet',
            q=query,
            type='video',
            eventType='completed',
            relevanceLanguage='en',
            publishedAfter=last_execution_date.replace(microsecond=0).isoformat() + 'Z'
        )

        search_response = search_request.execute()

        for item in search_response['items']:
            video_id = item['id']['videoId']
            channel_id = item['snippet']['channelId']

            video_request = self.youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            )

            video_response = video_request.execute()

            for video in video_response['items']:
                video_snippet = video['snippet']
                video_statistics = video['statistics']
                video_content_details = video['contentDetails']
                self.handle_channel_data(channel_id)
                self.process_video_data(query, video_id, video_snippet, video_statistics, video_content_details)

    def handle_channel_data(self, channel_id):
        if channel_id in self.channel_subscribers:
            channel_subscriber_count = self.channel_subscribers[channel_id]
        else:
            channel_request = self.youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            )
            channel_response = channel_request.execute()
            channel_subscriber_count = int(channel_response['items'][0]['statistics']['subscriberCount'])
            self.channel_subscribers[channel_id] = channel_subscriber_count
            self.process_channel_data(channel_id, channel_response)


    def process_channel_data(self, channel_id, channel_response):
        if self.channel_subscribers[channel_id] >= 1000:
            channel = channel_response['items'][0]
            channel_snippet = channel['snippet']
            channel_statistics = channel['statistics']
            channel_data = {
                "channel_id": channel_id,
                "channel_title": channel_snippet['title'],
                "channel_description": channel_snippet.get('description', ''),
                "subscriber_count": int(channel_statistics.get('subscriberCount', 0)),
                "video_count": int(channel_statistics.get('videoCount', 0)),
                "published_at": channel_snippet['publishedAt']
            }
            self.insert_channel_data(channel_data)

    def process_video_data(self, query, video_id, video_snippet, video_statistics, video_content_details):
        if self.channel_subscribers[video_snippet['channelId']] >= 1000:
            video_data = {
                "query": query,
                "video_id": video_id,
                "video_title": video_snippet['title'],
                "video_description": video_snippet['description'],
                "published_at": video_snippet['publishedAt'],
                "channel_id": video_snippet['channelId'],
                "view_count": int(video_statistics.get('viewCount', 0)),
                "like_count": int(video_statistics.get('likeCount', 0)),
                "comment_count": int(video_statistics.get('commentCount', 0)),
                "default_audio_language": video_snippet.get('defaultAudioLanguage', 'N/A'),
                "video_duration": video_content_details['duration'],
                "tags": video_snippet.get('tags', [])
            }
            self.insert_video_data(video_data)

    def insert_channel_data(self, channel_data):
        insert_query_channel = f"""
            INSERT INTO fin_test.youtube_channels (channel_id, channel_title, channel_description, subscriber_count, video_count, published_at) 
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        self.cursor.execute(insert_query_channel, (
            channel_data["channel_id"],
            channel_data["channel_title"],
            channel_data["channel_description"],
            channel_data["subscriber_count"],
            channel_data["video_count"],
            channel_data["published_at"]
        ))

    def insert_video_data(self, video_data):
        insert_query_video = f"""
            INSERT INTO fin_test.youtube_videos (query, video_id, video_title, video_description, published_at, channel_id, view_count, like_count, comment_count, default_audio_language, video_duration, tags) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        self.cursor.execute(insert_query_video, (
            video_data["query"],
            video_data["video_id"],
            video_data["video_title"],
            video_data["video_description"],
            video_data["published_at"],
            video_data["channel_id"],
            video_data["view_count"],
            video_data["like_count"],
            video_data["comment_count"],
            video_data["default_audio_language"],
            video_data["video_duration"],
            video_data["tags"]
        ))

    def get_last_execution_date(self):
        self.cursor.execute("SELECT * FROM fin_test.execution_timestamp")
        return self.cursor.fetchone()[0]

    def truncate_tables(self):
        self.cursor.execute("""TRUNCATE TABLE fin_test.youtube_channels""")
        self.cursor.execute("""TRUNCATE TABLE fin_test.youtube_videos""")

    def update_execution_timestamp(self):
        self.cursor.execute("""TRUNCATE TABLE fin_test.execution_timestamp""")
        self.cursor.execute(f"""INSERT INTO fin_test.execution_timestamp VALUES ('{datetime.now()}')""")


    def close_connection(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 6),
    'retries': 1,
}

queries = ["Power BI", "Power Query"]

with DAG('youtube_data_fetcher_dag', default_args=default_args, schedule_interval='@daily') as dag:
    fetch_data_task = YouTubeDataFetcherOperator(
        task_id='fetch_youtube_data',
        api_key='API',
        dbname='db',
        user='username',
        password='password',
        host='host',
        port='port',
        queries=queries
    )

    insert_into_dim_channels = PostgresOperator(
        task_id='insert_into_dim_channels',
        postgres_conn_id=postgres_conn_id,
        sql="""
            INSERT INTO fin_test.dim_channels  (channel_id, channel_title, channel_description, subscriber_count, video_count, published_at)
            SELECT channel_id, channel_title, channel_description, subscriber_count, video_count, published_at
            FROM fin_test.youtube_channels
            ON CONFLICT (channel_id) DO UPDATE
            SET 
                channel_title = EXCLUDED.channel_title,
                channel_description = EXCLUDED.channel_description,
                subscriber_count = EXCLUDED.subscriber_count,
                video_count = EXCLUDED.video_count,
                published_at = EXCLUDED.published_at;

            """
    )

    insert_into_dim_videos_details = PostgresOperator(
        task_id='insert_into_dim_videos_details',
        postgres_conn_id=postgres_conn_id,
        sql="""
            INSERT INTO fin_test.dim_videos_details (video_id, video_title, video_description, published_at, default_audio_language, video_duration, tags)
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
            """
    )

    insert_into_fact_videos = PostgresOperator(
        task_id='insert_into_fact_videos',
        postgres_conn_id=postgres_conn_id,
        sql="""
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
            """
    )


    fetch_data_task >>  [insert_into_dim_channels,insert_into_dim_videos_details] >> insert_into_fact_videos
