use anyhow::Context;
use kafka_protocol::messages::create_topics_request::{CreatableTopicBuilder, CreateTopicsRequestBuilder};
use kafka_protocol::messages::delete_topics_request::{DeleteTopicStateBuilder, DeleteTopicsRequestBuilder};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use kafka_rs::{Client, IndexMap};

const TEST_BROKER_ID: i32 = 1;

/// Clear the cluster of topics to ensure a clean slate for each test.
async fn delete_all_topics(cli: &Client) {
    let metadata = cli.get_metadata(TEST_BROKER_ID).await.context("error calling get_metadata").unwrap();
    println!("{:?}", metadata.clone());
    let mut topics = vec![];
    for topic in metadata.topics.keys() {
        let mut dtsb = DeleteTopicStateBuilder::default();
        dtsb.name(Some(topic.clone()));
        let dts = dtsb.build().unwrap();
        topics.push(dts);
    }
    // We're done if there are no topics.
    if topics.len() == 0 {
        return;
    }
    let mut dtrb = DeleteTopicsRequestBuilder::default();
    dtrb.topics(topics);
    let del_req = dtrb.build().unwrap();

    cli.admin().delete_topics(del_req).await.context("error deleting topics").unwrap();
}

/// Creates a new client.
fn get_client() -> Client {
    // TODO: test configuration. For now we hardcode.
    let host = "localhost";
    let port = "9092";
    Client::new(vec![format!("{}:{}", host, port)])
}

async fn setup_topic(cli: &Client, topic: &str) {
    let topic = StrBytes::from_string(topic.to_string());
    let mut ctb = CreatableTopicBuilder::default();
    ctb.replication_factor(1);
    ctb.num_partitions(1);
    let ct = ctb.build().unwrap();

    let mut ctrb = CreateTopicsRequestBuilder::default();
    let mut imap = IndexMap::new();
    imap.insert(TopicName(topic), ct);
    ctrb.topics(imap);
    let req = ctrb.build().unwrap();
    cli.admin().create_topics(req).await.unwrap();
}

#[tokio::test]
async fn create_topics() {
    // Assemble.
    let cli = get_client();
    delete_all_topics(&cli).await;

    let topic = StrBytes::from_static_str("test-create-topic");
    let mut ctb = CreatableTopicBuilder::default();
    ctb.replication_factor(1);
    ctb.num_partitions(1);
    let ct = ctb.build().unwrap();

    let mut ctrb = CreateTopicsRequestBuilder::default();
    let mut imap = IndexMap::new();
    imap.insert(TopicName(topic), ct);
    ctrb.topics(imap);
    let req = ctrb.build().unwrap();

    // Action.
    let res = cli.admin().create_topics(req).await.context("error creating topic");

    // Assert.
    assert!(!res.is_err())
}

#[tokio::test]
async fn delete_topics() {
    // Assemble.
    let cli = get_client();
    delete_all_topics(&cli).await;
    let topic_name = "test-delete-topic";
    setup_topic(&cli, topic_name).await;
    
    let topic = StrBytes::from_static_str(topic_name);
    let mut dtsb = DeleteTopicStateBuilder::default();
    dtsb.name(Some(TopicName(topic)));
    let dts = dtsb.build().unwrap();
    
    let topics = vec![dts];
    let mut dtrb = DeleteTopicsRequestBuilder::default();
    dtrb.topics(topics);
    let req = dtrb.build().unwrap();
    
    // Action.
    let res = cli.admin().delete_topics(req).await.context("error deleting topic");

    // Assert.
    assert!(!res.is_err());
}

#[tokio::test]
async fn get_metadata() {
    // Assemble.
    let cli = get_client();
    let broker_id = TEST_BROKER_ID;

    // Action.
    let res = cli.get_metadata(broker_id).await.context("error getting metadata");

    // Assert.
    assert!(!res.is_err());
    let metadata = res.unwrap();
    assert_eq!(metadata.controller_id, broker_id);
}
