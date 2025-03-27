from dataclasses import asdict, dataclass, field
from pydantic import BaseModel, Field
from typing import List, Tuple, Dict
import os

from jinja2 import Environment, FileSystemLoader
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# dependency jars to read data from kafka, and connect to postgres
REQUIRED_JARS = [
    "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.0.jar",
    "file:///opt/flink/lib/flink-connector-jdbc-3.0.0-1.16.jar",
    "file:///opt/flink/lib/postgresql-42.6.0.jar",
]


# Streaming Job Config Class
class StreamJobConfig:
    jars: List[str] = Field(default_factory=lambda: REQUIRED_JARS)
    checkpoint_interval: int = 10
    checkpoint_pause: int = 5 
    checkpoint_timeout: int = 5  
    parallelism: int = 2

@dataclass(frozen=True)
class KafkaConfig:
    connector: str = "kafka"
    bootstrap_servers: str = "kafka:9092"
    scan_startup_mode: str = "earliest-offset"
    consumer_group_id: str = "flink-consumer-group-1"

# Postgres Database Config Class
@dataclass(frozen=True)
class ApplicationDatabaseConfig:
    connector: str = "jdbc"
    url: str = "jdbc:postgresql://postgres:5432/postgres?currentSchema=ad_engagement"
    username: str = "postgres"
    password: str = "postgres"
    driver: str = "org.postgresql.Driver"


# Ad Impression TopicConfig Class
@dataclass(frozen=True)
class AdImpressionTopicConfig(KafkaConfig):
    topic: str = 'ad_interaction'
    format: str = 'json'

# Source DB Tables Config Class
@dataclass(frozen=True)
class ApplicationUsersTableConfig(ApplicationDatabaseConfig):
    table_name: str = 'ad_engagement.user_profiles'

@dataclass(frozen=True)
class ApplicationContentTableConfig(ApplicationDatabaseConfig):
    table_name: str = 'ad_engagement.content_catalog'

@dataclass(frozen=True)
class ApplicationAdsTableConfig(ApplicationDatabaseConfig):
    table_name: str = 'ad_engagement.ads_catalog'

# Sink DB Table Config Class
@dataclass(frozen=True)
class ApplicationAttributedAdPerformanceTableConfig(ApplicationDatabaseConfig):
    table_name: str = 'ad_engagement.attributed_ad_impressions'

def get_execution_environment(config: StreamJobConfig, job_name: str
) -> Tuple[StreamExecutionEnvironment, StreamTableEnvironment]:
    s_env = StreamExecutionEnvironment.get_execution_environment()
    
    # start a checkpoint every 10,000 ms (10 s)
    s_env.enable_checkpointing(config.checkpoint_interval * 1000)

    # make sure 5000 ms (5 s) of progress happen between checkpoints
    s_env.get_checkpoint_config().set_min_pause_between_checkpoints(
        config.checkpoint_pause * 1000
    )
    # checkpoints have to complete within 5 minute, or are discarded
    s_env.get_checkpoint_config().set_checkpoint_timeout(
        config.checkpoint_timeout * 1000
    )
    execution_config = s_env.get_config()
    execution_config.set_parallelism(config.parallelism)
    
    t_env = StreamTableEnvironment.create(s_env)

    job_config = t_env.get_config().get_configuration()
    job_config.set_string("pipeline.name", job_name)

    return s_env, t_env

def get_sql_query(
    entity: str,
    type: str,
    template_env: Environment = Environment(loader=FileSystemLoader("code/"))
) -> str:
    
    CONFIG_MAP = {
        "source_ad_interactions": AdImpressionTopicConfig(),
        "source_users": ApplicationUsersTableConfig(),
        "source_content_catalog": ApplicationContentTableConfig(),
        "source_ad_catalog": ApplicationAdsTableConfig(),
        "process_attribute_ad_impression": ApplicationAttributedAdPerformanceTableConfig(),
        "sink_attributed_ad_impression": ApplicationAttributedAdPerformanceTableConfig()
    }
    
    return template_env.get_template(f"{type}/{entity}.sql").render(asdict(CONFIG_MAP[entity]))

def run_adperformance_attribution_job(
    t_env: StreamTableEnvironment,
    get_sql_query=get_sql_query,
) -> None:
    
    # Create Source DDLs
    t_env.execute_sql(get_sql_query('source_ad_interactions','source'))
    t_env.execute_sql(get_sql_query('source_content_catalog','source'))
    t_env.execute_sql(get_sql_query('source_ad_catalog','source'))
    t_env.execute_sql(get_sql_query('source_users','source'))

    # Create Sink DDL
    t_env.execute_sql(get_sql_query('sink_attributed_ad_impression','sink'))
    
    #t_env.execute_sql("SHOW CATALOGS").print()

    #t_env.execute_sql("SHOW TABLES").print()

    # Run processing query
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(get_sql_query('process_attribute_ad_impression', 'process'))
    adperformance_attribution_job = stmt_set.execute()

    print(
        f"""
        Async attributed ads performance sink job
         status: {adperformance_attribution_job.get_job_client().get_job_status()}
        """
    )

if __name__ == '__main__':
    _, t_env = get_execution_environment(StreamJobConfig(), 'ad-performance-attribution')
    run_adperformance_attribution_job(t_env)





