#[allow(unused_imports)]
use skw_lib_shared::{
    APP, AppError,
    prelude::{
        chrono::*,
        consts::*,
        futures::StreamExt,
        iggy::*,
        jsonrpc::ServiceHttpError,
        log::{error, info, warn},
        strum::EnumString,
    },
    skw_get_consumer_name,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, atomic::AtomicU64},
};

use tokio::sync::broadcast::Sender;

#[derive(Clone)]
pub struct RoutesMap {
    pub map: HashMap<String, Arc<IggyProducer>>,
}

#[derive(Clone)]
pub struct GateState {
    pub auth_service_url: String,
    pub routes_map: RoutesMap,
    pub back_sender: Sender<IggyReceivedMessage>,
    pub _iggy_client: Arc<IggyClient>, // saved for future use if needed
    pub _cur_id: Arc<AtomicU64>,
}
#[derive(Debug, Clone)]
pub struct IggyReceivedMessage {
    pub payload: Vec<u8>,
    pub signature: Option<String>,
    pub service_error: Option<ServiceHttpError>,
    pub _ts_service_received: Option<DateTime<Utc>>,
    pub _ts_service_sent: Option<DateTime<Utc>>,
    pub _ts_gate_back_received: DateTime<Utc>,
}

impl TryFrom<ReceivedMessage> for IggyReceivedMessage {
    type Error = String;

    fn try_from(m: ReceivedMessage) -> Result<Self, Self::Error> {
        let ts_gate_back_received = Utc::now();

        let signature_key = HeaderKey::from_str(IGGY_HEADER_SIGNATURE).map_err(|e| e.to_string())?;

        let signature = m
            .message
            .get_user_header(&signature_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value());

        let service_error_key = HeaderKey::from_str(IGGY_HEADER_SERVICE_ERROR).map_err(|e| e.to_string())?;

        let service_error = m
            .message
            .get_user_header(&service_error_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value())
            .and_then(|v| v.parse::<ServiceHttpError>().ok());

        let ts_service_received_key = HeaderKey::from_str(IGGY_HEADER_TS_SERVICE_RECEIVED).map_err(|e| e.to_string())?;
        let ts_service_received = m
            .message
            .get_user_header(&ts_service_received_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value())
            .and_then(|v| DateTime::parse_from_rfc3339(&v).ok())
            .map(|v| v.with_timezone(&Utc));

        let ts_service_sent_key = HeaderKey::from_str(IGGY_HEADER_TS_SERVICE_SENT).map_err(|e| e.to_string())?;
        let ts_service_sent = m
            .message
            .get_user_header(&ts_service_sent_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value())
            .and_then(|v| DateTime::parse_from_rfc3339(&v).ok())
            .map(|v| v.with_timezone(&Utc));

        Ok(IggyReceivedMessage {
            payload: m.message.payload.to_vec(),
            signature,
            service_error,
            _ts_service_received: ts_service_received,
            _ts_service_sent: ts_service_sent,
            _ts_gate_back_received: ts_gate_back_received,
        })
    }
}

impl GateState {
    pub async fn new(
        auth_service_url: &str,
        stream: &str,
        back_topic: &str,
        routes_map: RoutesMap,
        arc_iggy_client: Arc<IggyClient>,
        back_topic_batch_length: u32,
        back_topic_consumers: u32,
    ) -> Result<Self, AppError> {
        // let consumer_name = format!("consumer-{}", back_topic);
        let consumer_name = skw_get_consumer_name(back_topic);

        let (ts, _) = tokio::sync::broadcast::channel::<IggyReceivedMessage>(1000);

        // Each back-topic consumer below is its own IggyClient connection (not a
        // clone of arc_iggy_client): the Iggy server tracks consumer-group
        // membership per connection, so N consumers sharing one connection would
        // all register as the same group member and the server would never split
        // partitions across them. Separate connections is what actually gets
        // partitions assigned in parallel across workers.
        for worker_id in 0..back_topic_consumers.max(1) {
            let stream = stream.to_string();
            let back_topic = back_topic.to_string();
            let consumer_name = consumer_name.clone();
            let consumer_ts = ts.clone();

            tokio::spawn(async move {
                loop {
                    let iggy = APP.iggy.clone().connect().await;
                    let Some(client) = iggy.inner() else {
                        error!("back receiver #{worker_id}: no iggy client available");
                        return;
                    };

                    let mut iggy_consumer = match client.consumer_group(&consumer_name, &stream, &back_topic) {
                        Ok(builder) => {
                            let poll_interval = match IggyDuration::from_str("1ms") {
                                Ok(i) => i,
                                Err(e) => {
                                    error!("back receiver #{worker_id}: invalid poll interval: {e}");
                                    return;
                                }
                            };
                            builder
                                .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
                                .create_consumer_group_if_not_exists()
                                .auto_join_consumer_group()
                                .polling_strategy(PollingStrategy::next())
                                .poll_interval(poll_interval)
                                .batch_length(back_topic_batch_length)
                                .build()
                        }
                        Err(e) => {
                            error!("back receiver #{worker_id}: failed to build consumer: {e}");
                            return;
                        }
                    };

                    if let Err(e) = iggy_consumer.init().await {
                        error!("back receiver #{worker_id}: failed to init consumer: {e}");
                        return;
                    }

                    info!(
                        "starting back receiver thread #{worker_id} {}",
                        consumer_name
                    );
                    while let Some(message) = iggy_consumer.next().await {
                        match message {
                            Ok(received) => match received.try_into() {
                                Ok(m) => {
                                    if let Err(e) = consumer_ts.send(m) {
                                        error!("tokio::sync::broadcast::channel error: {}", e)
                                    }
                                }
                                Err(e) => error!("error converting back message: {}", e),
                            },
                            Err(e) => error!("Error while receiving message: {}", e),
                        }
                    }
                }
            });
        }

        Ok(Self {
            auth_service_url: auth_service_url.into(),
            routes_map,
            back_sender: ts,
            _iggy_client: arc_iggy_client,
            _cur_id: Arc::new(AtomicU64::new(0)),
        })
    }
}
