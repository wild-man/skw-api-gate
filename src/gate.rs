use skw_lib_shared::skw_get_consumer_name;
#[allow(unused_imports)]
use skw_lib_shared::{
    APP, AppError,
    prelude::{
        consts::*,
        futures::StreamExt,
        iggy::*,
        jsonrpc::ServiceHttpError,
        log::{error, info, warn},
        strum::EnumString,
    },
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
    pub cur_id: Arc<AtomicU64>,
}
#[derive(Debug, Clone)]
pub struct IggyReceivedMessage {
    pub payload: Vec<u8>,
    pub signature: Option<String>,
    pub service_error: Option<ServiceHttpError>,
}

impl TryFrom<ReceivedMessage> for IggyReceivedMessage {
    type Error = String;

    fn try_from(m: ReceivedMessage) -> Result<Self, Self::Error> {
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

        Ok(IggyReceivedMessage {
            payload: m.message.payload.to_vec(),
            signature: signature,
            service_error,
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
    ) -> Result<Self, AppError> {
        // let consumer_name = format!("consumer-{}", back_topic);
        let consumer_name = skw_get_consumer_name(back_topic);

        let mut iggy_consumer = arc_iggy_client
            .consumer_group(&consumer_name, stream, back_topic)
            .map_err(|e| AppError::Custom(e.to_string()))?
            .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .polling_strategy(PollingStrategy::next())
            .poll_interval(IggyDuration::from_str("1ms").map_err(|e| AppError::Custom(e.to_string()))?)
            .batch_length(1)
            .build();

        iggy_consumer
            .init()
            .await
            .map_err(|e| AppError::Custom(e.to_string()))?;

        let (tx, _) = tokio::sync::broadcast::channel::<IggyReceivedMessage>(10000);
        let consumer_tx = tx.clone();

        let consumer_name1 = consumer_name.clone();
        tokio::spawn(async move {
            loop {
                info!("starting back receiver thread {}", consumer_name1);
                while let Some(message) = iggy_consumer.next().await {
                    match message {
                        Ok(received) => {
                            if let Ok(m) = received.try_into() {
                                if let Err(e) = consumer_tx.send(m) {
                                    error!("tokio::sync::broadcast::channel error: {}", e.to_string())
                                };
                            }
                        }

                        Err(error) => error!("Error while receiving message: {error}"),
                    }
                }
            }
        });

        let consumer_name2 = consumer_name.clone();
        let consumer_tx = tx.clone();
        tokio::spawn(async move {
            info!("starting one back receiver thread {}", consumer_name2);

            let mut rx = consumer_tx.subscribe();
            loop {
                match rx.recv().await {
                    _ => continue,
                }
            }
        });

        Ok(Self {
            auth_service_url: auth_service_url.into(),
            routes_map,
            back_sender: tx,
            _iggy_client: arc_iggy_client,
            cur_id: Arc::new(AtomicU64::new(0)),
        })
    }
}
