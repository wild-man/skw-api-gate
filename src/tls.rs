use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use skw_lib_shared::APP;
use skw_lib_shared::prelude::consts::{TLS_CERT_PATH, TLS_KEY_PATH};

pub fn build_rustls_config() -> ServerConfig {
    let cert_path = APP.config.expect_string(TLS_CERT_PATH);
    let key_path = APP.config.expect_string(TLS_KEY_PATH);

    let cert_chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
        .unwrap_or_else(|_| panic!("reading cert file {cert_path}"))
        .collect::<Result<_, _>>()
        .unwrap_or_else(|_| panic!("parsing cert file {cert_path}"));

    let key = PrivateKeyDer::from_pem_file(&key_path).unwrap_or_else(|_| panic!("reading key file {key_path}"));

    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .expect("building rustls ServerConfig from cert/key")
}
