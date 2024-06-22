use anyhow::{Context, Result};
use kafka_protocol::messages::create_topics_request::{CreatableTopicBuilder, CreateTopicsRequestBuilder};
use kafka_protocol::messages::delete_topics_request::{DeleteTopicStateBuilder, DeleteTopicsRequestBuilder};
use kafka_protocol::messages::TopicName;
use kafka_rs::{Client, IndexMap, StrBytes};
use tracing_subscriber::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        // Filter spans based on the RUST_LOG env var.
        .with(tracing_subscriber::EnvFilter::new("error,delete_topic=debug,kafka_rs=debug"))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false) // Too verbose, so disable target.
                .with_level(true) // Shows tracing level: debug, info, error &c.
                .compact(),
        )
        // Install this registry as the global tracing registry.
        .try_init()
        .expect("error initializing tracing");

    let topic = StrBytes::from_string(std::env::var("TOPIC").context("TOPIC env var must be specified")?);
    let host = std::env::var("HOST").context("HOST env var must be specified")?;
    let port = std::env::var("PORT").context("PORT env var must be specified")?;
    let cli = Client::new(vec![format!("{}:{}", host, port)]);

    // Create new topic.
    let admin = cli.admin();
    let mut ctb = CreatableTopicBuilder::default();
    ctb.replication_factor(1);
    ctb.num_partitions(1);
    let ct = ctb.build()?;

    let mut ctrb = CreateTopicsRequestBuilder::default();
    let mut imap = IndexMap::new();
    imap.insert(TopicName(topic), ct);
    ctrb.topics(imap);
    let req = ctrb.build()?;

    admin.create_topics(req).await.context("error creating topic")?;

    // Now delete it!
    let topic = StrBytes::from_string(std::env::var("TOPIC").context("TOPIC env var must be specified")?);
    let mut dtsb = DeleteTopicStateBuilder::default();
    dtsb.name(Some(TopicName(topic)));
    let dts = dtsb.build()?;

    let topics = vec![dts];
    let mut dtrb = DeleteTopicsRequestBuilder::default();
    dtrb.topics(topics);
    let req = dtrb.build()?;

    admin.delete_topics(req).await.context("error deleting topic")?;

    Ok(())
}
