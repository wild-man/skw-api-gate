use std::{collections::HashMap, str::FromStr, sync::Arc};

// give minimal improvements
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub(crate) const CFG_PATH_BACK_TOPIC_PART_COUNT: &str = "/service/backTopicPartitionsCount";
pub(crate) const CFG_PATH_BACK_TOPIC_BATCH_LENGTH: &str = "/service/backTopicBatchLength";
pub(crate) const CFG_PATH_BACK_TOPIC_CONSUMERS: &str = "/service/backTopicConsumers";

use ntex::{
    SharedCfg,
    http::HttpServiceConfig,
    time::Seconds,
    web::{self, App, HttpServer},
};

use skw_lib_shared::{
    APP, AppError,
    prelude::{
        consts::*,
        iggy::*,
        log::{info, warn},
        tools::*,
    },
};

use crate::gate::{GateState, RoutesMap};

mod auth;
mod cors;
mod gate;
mod tls;

#[ntex::main]
async fn main() -> Result<(), AppError> {
    let bind_addr = APP.config.expect_string(SERVICE_BIND_ADDRESS);
    let back_topic = skw_get_back_topic_name();
    let stream = skw_get_stream_name();

    let workers = APP
        .config
        .get(SERVICE_WORKERS)
        .map(|w| w.as_u64().expect("workers is not a number") as usize)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(2, std::num::NonZeroUsize::get));

    let auth_service_url = APP
        .config
        .get(AUTH_SREVICE_URL)
        .map(|a| {
            a.as_str()
                .expect("invalid /service/authServiceUrl")
                .to_string()
        })
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| skw_lib_http_protos::auth::DEFAULT_SERVICE_URL.to_string());
    let auth_service_url = format!("{}{}", auth_service_url, INTERNAL_URL_PREFIX);

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("unable to install rustls aws-lc-rs CryptoProvider");

    let tls_config = tls::build_rustls_config();
    cors::init();

    //@todo move values into config
    let http_cfg = HttpServiceConfig::new()
        .set_client_timeout(Seconds::new(3))
        .set_keepalive_timeout(Seconds::new(20))
        .set_headers_read_rate(Seconds::new(1), Seconds::new(3), 1)
        .set_payload_read_rate(Seconds::new(5), Seconds::new(10), 1);

    let shared_http_cfg = SharedCfg::new("api-gate").add(http_cfg);

    let iggy = APP.iggy.clone().connect().await;
    let iggy_client = iggy.inner().expect("bad iggy client");

    let routes_map = init_iggy(&stream, &back_topic, &iggy_client).await?;

    let bt_batch_length = APP.config.expect_u32(CFG_PATH_BACK_TOPIC_BATCH_LENGTH);
    let bt_consumers = APP.config.expect_u32(CFG_PATH_BACK_TOPIC_CONSUMERS);

    let gate_state = GateState::new(
        &auth_service_url,
        &stream,
        &back_topic,
        routes_map,
        iggy_client,
        bt_batch_length,
        bt_consumers,
    )
    .await?;

    info!("starting skw-api-gate listening on https://{}", bind_addr);
    HttpServer::new(async move || {
        App::new()
            .state(gate_state.clone())
            .default_service(web::route().to(gate::catch_all))
    })
    .bind_rustls(&bind_addr, &tls_config)
    .map_err(|e| AppError::Custom(e.to_string()))?
    .config(shared_http_cfg)
    .workers(workers)
    .run()
    .await
    .map_err(|e| AppError::Custom(e.to_string()))?;

    Ok(())
}

async fn init_iggy(stream: &str, back_topic: &str, iggy_client: &IggyClient) -> Result<RoutesMap, AppError> {
    let stream_id = Identifier::from_str_value(stream).expect("invalid stream name");

    match iggy_client.create_stream(stream).await {
        Ok(sd) => info!("iggy stream {} created; id:{}", stream, sd.id),
        Err(e) => warn!("failed to create iggy stream {}: {}", stream, e),
    };
    let partitions_count = APP.config.expect_u32(CFG_PATH_BACK_TOPIC_PART_COUNT);

    match iggy_client
        .create_topic(
            &stream_id,
            back_topic,
            partitions_count,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ExpireDuration(IggyDuration::from_str("60s").unwrap()),
            MaxTopicSize::ServerDefault,
        )
        .await
    {
        Ok(tc) => info!("gate-back topic created: {:?}", tc),
        Err(e) => warn!("Error gate-back topic creation: {}", e),
    }

    let mut routes_map = RoutesMap {
        map: HashMap::with_capacity(100),
    };

    match APP.config.get(GATE_TOPICS_MAP) {
        Some(t) => {
            for (service, topic_cfg) in t.as_object().expect("invalid topic configuration").iter() {
                // dbg!((service_name, topic_cfg));
                let cfg: TopicConfig = topic_cfg.try_into()?;
                for topic_type in &cfg.topic_types {
                    let topic = format!("{}-{}-{}", service, topic_type, VERSION);
                    let topic_key = format!("{service}/{topic_type}");

                    skw_is_valid_name(&topic).unwrap_or_else(|| panic!("invalid topic name: {}", topic));

                    create_iggy_topic(
                        iggy_client,
                        &mut routes_map,
                        &cfg,
                        service,
                        &stream_id,
                        stream,
                        &topic_key,
                        &topic,
                    )
                    .await;
                }
            }
        }
        None => warn!("no topics configurations found in config"),
    }

    Ok(routes_map)
}

#[allow(clippy::too_many_arguments)]
async fn create_iggy_topic(
    iggy_client: &IggyClient,
    routes_map: &mut RoutesMap,
    cfg: &TopicConfig,
    service: &str,
    stream_id: &Identifier,
    stream: &str,
    topic_key: &str,
    topic: &str,
) {
    match iggy_client
        .create_topic(
            stream_id,
            topic,
            cfg.partitions_count,
            cfg.compression_algorithm,
            None,
            cfg.message_expiry,
            cfg.max_topic_size,
        )
        .await
    {
        Ok(t) => info!("created topic: {} (id:{}); stream: {}", topic, t.id, stream),
        Err(e) => warn!(
            "error create topic: {}; stream: {}; error: {}",
            topic, stream, e
        ),
    }

    info!(
        "creating producer for service: {}; stream: {}; topic: {};",
        service, stream, topic
    );

    let producer = iggy_client
        .producer(stream, topic)
        .expect("producer create error") // stream: "orders", topic: "events"
        .partitioning(Partitioning::balanced()) // Automatically balances across partitions
        .build();

    routes_map
        .map
        .entry(topic_key.to_string())
        .or_insert(Arc::new(producer));
}
