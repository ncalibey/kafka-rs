//! Crate error types.

use kafka_protocol::{messages::RequestKind, ResponseError};

use crate::StrBytes;

/// Client results from interaction with a Kafka cluster.
pub type ClientResult<T> = std::result::Result<T, ClientError>;

/// Client errors from interacting with a Kafka cluster.
///
/// TODO: probably just refactor this into an opaque Retryable and Fatal errors, which just dump info on debug.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Error while interacting with a broker.
    #[error("error while interacting with a broker: {0:?}")]
    BrokerError(BrokerRequestError),
    /// Error while encoding a batch of records.
    #[error("error while encoding a batch of records: {0}")]
    EncodingError(String),
    /// The broker returned a malformed response.
    #[error("broker returned a malformed response")]
    MalformedResponse,
    /// The specified topic has no available partitions.
    #[error("the specified topic has no available partitions: {0}")]
    NoPartitionsAvailable(String),
    /// Produce requests must include at least 1 record.
    #[error("produce requests must include at least 1 record")]
    ProducerMessagesEmpty,
    /// The specified topic is unknown to the cluster.
    #[error("the specified topic is unknown to the cluster: {0}")]
    UnknownTopic(String),
    /// The specified topic partition is unknown to the cluster.
    #[error("the specified topic partition is unknown to the cluster: {0}/{1}")]
    UnknownPartition(String, i32),
    /// The specified topic partition is does not currently have a known leader.
    #[error("the specified topic partition is does not currently have a known leader: {0}/{1}")]
    NoPartitionLeader(String, i32),
    /// An error was returned in a response from a broker.
    #[error("an error was returned in a response from a broker: {0} {1:?} {2:?}")]
    ResponseError(i16, Option<ResponseError>, Option<StrBytes>),
    /// Timeout while waiting for cluster metadata to bootstrap.
    #[error("timeout while waiting for cluster metadata to bootstrap")]
    ClusterMetadataTimeout,
    /// Could not find a broker specified by ID, or any broker at all.
    #[error("could not find a broker specified by ID, or any broker at all")]
    NoBrokerFound,
    #[error("{0}")]
    Other(String),
    #[error("no topics were specified in the request")]
    NoTopicsSpecified,
    #[error("no controller was found in the cluster metadata")]
    NoControllerFound,
}

/// Broker connection level error.
#[derive(Debug, thiserror::Error)]
#[error("broker connection error: {kind:?}")]
pub struct BrokerRequestError {
    /// The original request payload.
    pub(crate) payload: RequestKind,
    /// The kind of error which has taken place.
    pub(crate) kind: BrokerErrorKind,
}

/// Broker connection level error kind.
#[derive(Debug, thiserror::Error)]
pub enum BrokerErrorKind {
    /// The connection to the broker has terminated.
    #[error("the client is disconnected")]
    Disconnected,
    /// The broker returned a malformed response.
    #[error("the broker returned a malformed response")]
    MalformedBrokerResponse,
}
