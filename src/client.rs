//! Kafka client implementation.

use std::{sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    indexmap::IndexMap,
    messages::{
        create_topics_request::CreateTopicsRequest,
        create_topics_response::CreateTopicsResponse,
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::DeleteTopicsResponse,
        fetch_request::{FetchPartition, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        produce_request::PartitionProduceData,
        FetchRequest, FindCoordinatorRequest, FindCoordinatorResponse, ListOffsetsRequest, MetadataResponse, ProduceRequest, ResponseHeader, ResponseKind,
    },
    protocol::StrBytes,
    records::{Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType},
    ResponseError,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::clitask::{ClientTask, Cluster, ClusterMeta, MetadataPolicy, Msg};
use crate::error::ClientError;
#[cfg(feature = "internal")]
use crate::internal::InternalClient;
use crate::{broker::BrokerResponse, error::ClientResult};

/*
- TODO: build a TopicBatcher on top of a TopicProducer which works like a sink,
  batches based on configured max size, will handle retries, and the other good stuff.
*/

/// The default timeout used for requests (10s).
pub const DEFAULT_TIMEOUT: i32 = 10 * 1000;

/// Headers of a message.
pub type MessageHeaders = IndexMap<StrBytes, Option<Bytes>>;

/// A Kafka client.
///
/// This client is `Send + Sync + Clone`, and cloning this client to share it among application
/// tasks is encouraged.
///
/// This client spawns a task which manages the full lifecycle of interaction with a Kafka cluster,
/// include initial broker connections based on seed list, cluster metadata discovery, connections
/// to new brokers, establish API versions of brokers, handling reconnects, and anything else
/// related to maintain connections to a Kafka cluster.
#[derive(Clone)]
pub struct Client {
    /// The channel used for communicating with the client task.
    pub(crate) tx: mpsc::Sender<Msg>,
    /// Discovered metadata on a Kafka cluster along with broker connections.
    ///
    /// NOTE WELL: this value should never be updated outside of the `ClientTask`.
    pub(crate) cluster: ClusterMeta,
    /// The shutdown signal for this client, which will be triggered once all client handles are dropped.
    _shutdown: Arc<DropGuard>,
}

impl Client {
    /// Construct a new instance.
    pub fn new(seed_list: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel(1_000);
        let shutdown = CancellationToken::new();
        let task = ClientTask::new(seed_list, None, MetadataPolicy::default(), rx, false, shutdown.clone());
        let topics = task.cluster.clone();
        tokio::spawn(task.run());
        Self {
            tx,
            cluster: topics,
            _shutdown: Arc::new(shutdown.drop_guard()),
        }
    }

    /// Construct a new instance from a seed list and a block list.
    ///
    /// - `seed_list`: URLs of the initial brokers to attempt to connect to in order to gather
    ///   cluster metadata. The metadata will be used to establish connections to all other brokers.
    /// - `block_list`: an optional set of broker IDs for which connections should not be established.
    ///   This is typically not needed. This only applies after cluster metadata has been gathered.
    ///
    /// The metadata policy is set to `Manual` for this constructor.
    #[cfg(feature = "internal")]
    pub fn new_internal(seed_list: Vec<String>, block_list: Vec<i32>) -> InternalClient {
        let (tx, rx) = mpsc::channel(1_000);
        let shutdown = CancellationToken::new();
        let task = ClientTask::new(seed_list, Some(block_list), MetadataPolicy::Manual, rx, true, shutdown.clone());
        let cluster = task.cluster.clone();
        tokio::spawn(task.run());
        let cli = Self {
            tx,
            cluster,
            _shutdown: Arc::new(shutdown.drop_guard()),
        };
        InternalClient::new(cli)
    }

    /// Get cluster metadata from the broker specified by ID.
    pub async fn get_metadata(&self, broker_id: i32) -> ClientResult<MetadataResponse> {
        let cluster = self.get_cluster_metadata_cache().await?;
        let broker = cluster
            .brokers
            .get(&broker_id)
            .ok_or(ClientError::Other("broker does not exist in currently discovered metadata".into()))?;

        // Send request.
        let uid = uuid::Uuid::new_v4();
        let (tx, rx) = oneshot::channel();
        broker.conn.get_metadata(uid, tx.into(), true).await;

        // Await response.
        let res = unpack_broker_response(rx).await.and_then(|(_, res)| {
            if let ResponseKind::MetadataResponse(res) = res {
                Ok(res)
            } else {
                Err(ClientError::MalformedResponse)
            }
        })?;

        Ok(res)
    }

    /// List topic partition offsets.
    pub async fn list_offsets(&self, topic: StrBytes, ptn: i32, pos: ListOffsetsPosition) -> ClientResult<i64> {
        let cluster = self.get_cluster_metadata_cache().await?;

        // Get the broker responsible for the target topic/partition.
        let topic_ptns = cluster.topics.get(&topic).ok_or(ClientError::UnknownTopic(topic.to_string()))?;
        let ptn_meta = topic_ptns.get(&ptn).ok_or(ClientError::UnknownPartition(topic.to_string(), ptn))?;
        let broker = ptn_meta.leader.clone().ok_or(ClientError::NoPartitionLeader(topic.to_string(), ptn))?;

        // Build request.
        let uid = uuid::Uuid::new_v4();
        let mut req = ListOffsetsRequest::default();
        // req.isolation_level = 0; // TODO: update this.
        let mut req_topic = ListOffsetsTopic::default();
        req_topic.name = topic.clone().into();
        let mut req_ptn = ListOffsetsPartition::default();
        req_ptn.partition_index = ptn;
        req_ptn.timestamp = match pos {
            ListOffsetsPosition::Earliest => -2,
            ListOffsetsPosition::Latest => -1,
            ListOffsetsPosition::Timestamp(val) => val,
        };
        req_topic.partitions.push(req_ptn);
        req.topics.push(req_topic);

        // Send request.
        let (tx, rx) = oneshot::channel();
        broker.conn.list_offsets(uid, req, tx).await;

        // Unpack response & handle errors.
        // TODO: check for error codes in response.
        let offset = unpack_broker_response(rx)
            .await
            .and_then(|(_, res)| {
                if let ResponseKind::ListOffsetsResponse(res) = res {
                    Ok(res)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })
            .and_then(|res| {
                res.topics
                    .iter()
                    .find(|topic_res| topic_res.name.0 == topic)
                    .and_then(|topic_res| topic_res.partitions.iter().find(|ptn_res| ptn_res.partition_index == ptn).map(|ptn_res| ptn_res.offset))
                    .ok_or(ClientError::MalformedResponse)
            })?;

        Ok(offset)
    }

    /// Fetch a batch of records from the target topic partition.
    pub async fn fetch(&self, topic: StrBytes, ptn: i32, start: i64) -> ClientResult<Option<Vec<Record>>> {
        let cluster = self.get_cluster_metadata_cache().await?;

        // Get the broker responsible for the target topic/partition.
        let topic_ptns = cluster.topics.get(&topic).ok_or(ClientError::UnknownTopic(topic.to_string()))?;
        let ptn_meta = topic_ptns.get(&ptn).ok_or(ClientError::UnknownPartition(topic.to_string(), ptn))?;
        let broker = ptn_meta.leader.clone().ok_or(ClientError::NoPartitionLeader(topic.to_string(), ptn))?;

        // Build request.
        let uid = uuid::Uuid::new_v4();
        let mut req = FetchRequest::default();
        req.max_bytes = 1024i32.pow(2);
        req.max_wait_ms = 10_000;
        // req.isolation_level = 0; // TODO: update this.
        let mut req_topic = FetchTopic::default();
        req_topic.topic = topic.clone().into();
        let mut req_ptn = FetchPartition::default();
        req_ptn.partition = ptn;
        req_ptn.partition_max_bytes = 1024i32.pow(2);
        req_ptn.fetch_offset = start;
        req_topic.partitions.push(req_ptn);
        req.topics.push(req_topic);
        tracing::debug!("about to send request: {:?}", req);

        // Send request.
        let (tx, rx) = oneshot::channel();
        broker.conn.fetch(uid, req, tx).await;

        // Unpack response & handle errors.
        // TODO: check for error codes in response.
        let batch_opt = unpack_broker_response(rx)
            .await
            .and_then(|(_, res)| {
                tracing::debug!("res: {:?}", res);
                if let ResponseKind::FetchResponse(res) = res {
                    Ok(res)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })
            .and_then(|res| {
                res.responses
                    .iter()
                    .find(|topic_res| topic_res.topic.0 == topic)
                    .and_then(|topic_res| topic_res.partitions.iter().find(|ptn_res| ptn_res.partition_index == ptn).map(|ptn_res| ptn_res.records.clone()))
                    .ok_or(ClientError::MalformedResponse)
            })?;

        // If some data was returned, then decode the batch.
        let Some(mut batch) = batch_opt else { return Ok(None) };
        let records = RecordBatchDecoder::decode(&mut batch).map_err(|_| ClientError::MalformedResponse)?;

        Ok(Some(records))
    }

    /// Get the cached cluster metadata.
    ///
    /// If the cluster metadata has not yet been bootstrapped, then this routine will wait for
    /// a maximum of 10s for the metadata to be bootstrapped, and will then timeout.
    pub(crate) async fn get_cluster_metadata_cache(&self) -> ClientResult<Arc<Cluster>> {
        let mut cluster = self.cluster.load();
        if !*cluster.bootstrap.borrow() {
            let mut sig = cluster.bootstrap.clone();
            let _ = tokio::time::timeout(Duration::from_secs(10), sig.wait_for(|val| *val))
                .await
                .map_err(|_err| ClientError::ClusterMetadataTimeout)?
                .map_err(|_err| ClientError::ClusterMetadataTimeout)?;
            cluster = self.cluster.load();
        }
        Ok(cluster.clone())
    }

    /// Find the coordinator for the given group.
    pub async fn find_coordinator(&self, key: StrBytes, key_type: i8, broker_id: Option<i32>) -> ClientResult<FindCoordinatorResponse> {
        let cluster = self.get_cluster_metadata_cache().await?;

        // Get the specified broker connection, else get the first available.
        let broker = broker_id
            .and_then(|id| cluster.brokers.get(&id).cloned())
            .or_else(|| {
                // TODO: get random broker.
                cluster.brokers.first_key_value().map(|(_, broker)| broker.clone())
            })
            .ok_or_else(|| ClientError::NoBrokerFound)?;

        // Build request.
        let uid = uuid::Uuid::new_v4();
        let mut req = FindCoordinatorRequest::default();
        req.key = key;
        req.key_type = key_type;

        // Send request.
        let (tx, rx) = oneshot::channel();
        broker.conn.find_coordinator(uid, req, tx).await;

        // Unpack response & handle errors.
        unpack_broker_response(rx)
            .await
            .and_then(|(_, res)| {
                tracing::debug!("res: {:?}", res);
                if let ResponseKind::FindCoordinatorResponse(res) = res {
                    Ok(res)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })
            .and_then(|res| {
                // Handle broker response codes.
                if res.error_code != 0 {
                    return Err(ClientError::ResponseError(res.error_code, ResponseError::try_from_code(res.error_code), res.error_message));
                }
                Ok(res)
            })
    }

    /// Build a producer for a topic.
    pub fn topic_producer(&self, topic: &str, acks: Acks, timeout_ms: Option<i32>, compression: Option<Compression>) -> TopicProducer {
        let (tx, rx) = mpsc::unbounded_channel();
        let compression = compression.unwrap_or(Compression::None);
        let encode_opts = RecordEncodeOptions { version: 2, compression };
        TopicProducer {
            _client: self.clone(),
            tx,
            rx,
            cluster: self.cluster.clone(),
            topic: StrBytes::from_string(topic.into()),
            acks,
            timeout_ms: timeout_ms.unwrap_or(DEFAULT_TIMEOUT),
            encode_opts,
            buf: BytesMut::with_capacity(1024 * 1024),
            batch_buf: Vec::with_capacity(1024),
            last_ptn: -1,
        }
    }

    /// Build an admin client.
    pub fn admin(&self) -> Admin {
        Admin { _client: self.clone() }
    }
}

/// A message to be encoded as a Kafka record within a record batch.
pub struct Message {
    /// An optional key for the record.
    pub key: Option<Bytes>,
    /// An optional value as the body of the record.
    pub value: Option<Bytes>,
    /// Optional headers to be included in the record.
    pub headers: MessageHeaders,
}

impl Message {
    /// Construct a new record.
    pub fn new(key: Option<Bytes>, value: Option<Bytes>, headers: MessageHeaders) -> Self {
        Self { key, value, headers }
    }
}

/// Write acknowledgements required for a request.
#[derive(Clone, Copy)]
#[repr(i16)]
pub enum Acks {
    /// Leader and replicas.
    All = -1,
    /// None.
    None = 0,
    /// Leader only.
    Leader = 1,
}

/// A producer for a specific topic.
pub struct TopicProducer {
    /// The client handle from which this producer was created.
    _client: Client,
    /// The channel used by this producer.
    tx: mpsc::UnboundedSender<BrokerResponse>,
    /// The channel used by this producer.
    rx: mpsc::UnboundedReceiver<BrokerResponse>,
    /// Discovered metadata on a Kafka cluster along with broker connections.
    ///
    /// NOTE WELL: this value should never be updated outside of the `ClientTask`.
    cluster: ClusterMeta,
    /// The topic being produced to.
    pub topic: StrBytes,
    /// Acks level to use for produce requests.
    acks: Acks,
    /// Timeout for produce requests.
    timeout_ms: i32,
    /// Record batch encoding config.
    encode_opts: RecordEncodeOptions,

    /// A buffer used for encoding batches.
    buf: BytesMut,
    /// A buffer to accumulating records to be sent to a broker.
    batch_buf: Vec<Record>,
    /// The last partition used for round-robin or uniform sticky batch assignment.
    last_ptn: i32,
}

impl TopicProducer {
    /// Produce a batch of records to the specified topic.
    pub async fn produce(&mut self, messages: &[Message]) -> ClientResult<(i64, i64)> {
        // TODO: allow for producer to specify partition instead of using sticky rotating partitions.

        // Check for topic metadata.
        if messages.is_empty() {
            return Err(ClientError::ProducerMessagesEmpty);
        }
        self.batch_buf.clear(); // Ensure buf is clear.
        let mut cluster = self.cluster.load();
        if !*cluster.bootstrap.borrow() {
            let mut sig = cluster.bootstrap.clone();
            let _ = sig.wait_for(|val| *val).await; // Ensure the cluster metadata is bootstrapped.
            cluster = self.cluster.load();
        }
        let Some(topic_ptns) = cluster.topics.get(&self.topic) else {
            return Err(ClientError::UnknownTopic(self.topic.to_string()));
        };

        // Target the next partition of this topic for this batch.
        let Some((sticky_ptn, sticky_broker)) = topic_ptns
            .range((self.last_ptn + 1)..)
            .filter_map(|(ptn, meta)| meta.leader.clone().map(|leader| (ptn, leader)))
            .next()
            .or_else(|| topic_ptns.range(..).filter_map(|(ptn, meta)| meta.leader.clone().map(|leader| (ptn, leader))).next())
            .map(|(key, val)| (*key, val.clone()))
        else {
            return Err(ClientError::NoPartitionsAvailable(self.topic.to_string()));
        };
        self.last_ptn = sticky_ptn;

        // Transform the given messages into their record form.
        let timestamp = chrono::Utc::now().timestamp_millis();
        for msg in messages.iter() {
            self.batch_buf.push(Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: 0,
                producer_epoch: 0,
                timestamp,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: 0,
                key: msg.key.clone(),
                value: msg.value.clone(),
                headers: msg.headers.clone(),
            });
        }

        // Encode the records into a request. Note that BytesMut will allocate more space whenever needed.
        let res = RecordBatchEncoder::encode(&mut self.buf, self.batch_buf.iter(), &self.encode_opts).map_err(|err| ClientError::EncodingError(format!("{:?}", err)));
        self.batch_buf.clear();
        res?;

        // Create the request object for the broker.
        let mut req = ProduceRequest::default();
        req.acks = self.acks as i16;
        req.timeout_ms = self.timeout_ms;
        let topic = req.topic_data.entry(self.topic.clone().into()).or_default();
        let mut ptn_data = PartitionProduceData::default();
        ptn_data.index = sticky_ptn;
        ptn_data.records = Some(self.buf.split().freeze());
        topic.partition_data.push(ptn_data);

        // Send off request & await response.
        let uid = uuid::Uuid::new_v4();
        sticky_broker.conn.produce(uid, req, self.tx.clone()).await;
        let res = loop {
            let Some(res) = self.rx.recv().await else {
                unreachable!("both ends of channel are held, receiving None should not be possible")
            };
            if res.id == uid {
                break res;
            }
        };

        // Handle response.
        // TODO: check for error codes in response.
        res.result
            .map_err(ClientError::BrokerError)
            .and_then(|res| {
                // Unpack the expected response type.
                if let ResponseKind::ProduceResponse(inner) = res.1 {
                    Ok(inner)
                } else {
                    tracing::error!("expected broker to return a ProduceResponse, got: {:?}", res.1);
                    Err(ClientError::MalformedResponse)
                }
            })
            .and_then(|res| {
                // Unpack the base offset & calculate the final offset.
                res.responses
                    .iter()
                    .find(|topic| topic.0 .0 == self.topic)
                    .and_then(|val| {
                        val.1.partition_responses.first().map(|val| {
                            debug_assert!(!messages.is_empty(), "messages len should always be validated at start of function");
                            let last_offset = val.base_offset + (messages.len() - 1) as i64;
                            (val.base_offset, last_offset)
                        })
                    })
                    .ok_or(ClientError::MalformedResponse)
            })
    }
}

/// The position in a log to fetch an offset for.
pub enum ListOffsetsPosition {
    /// Fetch the offset of the beginning of the partition's log.
    Earliest,
    /// Fetch the next offset after the last offset of the partition's log.
    Latest,
    /// Fetch the offset for the corresponding timestamp.
    Timestamp(i64),
}

/// Await and unpack a broker response.
///
/// TODO: turn this into a macro which can destructure to the match enum variant needed.
/// That will help reduce error handling boilerplate even further.
pub(crate) async fn unpack_broker_response(rx: oneshot::Receiver<BrokerResponse>) -> ClientResult<(ResponseHeader, ResponseKind)> {
    rx.await
        .map_err(|_| ClientError::Other("response channel dropped by broker, which should never happen".into()))?
        .result
        .map_err(ClientError::BrokerError)
}

pub struct Admin {
    /// The client handle from which this producer was created.
    _client: Client,
}

impl Admin {
    /// Creates new topics in the Kafka Cluster.
    pub async fn create_topics(&self, request: CreateTopicsRequest) -> ClientResult<CreateTopicsResponse> {
        if request.topics.is_empty() {
            return Err(ClientError::NoTopicsSpecified);
        }
        let cluster = self._client.get_cluster_metadata_cache().await?;
        let (tx, rx) = oneshot::channel();

        return if let Some(leader) = &cluster.controller {
            let uid = uuid::Uuid::new_v4();
            leader.conn.create_topics(uid, request, tx).await;
            unpack_broker_response(rx).await.and_then(|(_, res)| {
                if let ResponseKind::CreateTopicsResponse(inner) = res {
                    Ok(inner)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })
        } else {
            Err(ClientError::NoControllerFound)
        };
    }

    /// Delete topics from the Kafka Cluster.
    pub async fn delete_topics(&self, request: DeleteTopicsRequest) -> ClientResult<DeleteTopicsResponse> {
        if request.topics.is_empty() {
            return Err(ClientError::NoTopicsSpecified);
        }
        let cluster = self._client.get_cluster_metadata_cache().await?;
        let (tx, rx) = oneshot::channel();

        return if let Some(leader) = &cluster.controller {
            let uid = uuid::Uuid::new_v4();
            leader.conn.delete_topics(uid, request, tx).await;
            unpack_broker_response(rx).await.and_then(|(_, res)| {
                if let ResponseKind::DeleteTopicsResponse(inner) = res {
                    Ok(inner)
                } else {
                    Err(ClientError::MalformedResponse)
                }
            })
        } else {
            Err(ClientError::NoControllerFound)
        };
    }
}
