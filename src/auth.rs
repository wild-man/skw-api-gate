use ed25519_dalek::pkcs8::DecodePublicKey;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use ntex::web::HttpRequest;
use skw_lib_shared::{
    AppError,
    prelude::{
        base64::*,
        jsonrpc::{ServiceHttpError, internal_http_request},
        uuid::Uuid,
    },
};

use skw_lib_http_protos::auth::internal::*;

const API_KEY_HEADER: &str = "API-KEY";
const API_PAYLOAD_SIGNATURE_HEADER: &str = "PAYLOAD-SIGNATURE";

pub(crate) async fn check_auth_key_and_request_signature(url: &str, req: &HttpRequest, body: &str) -> Result<(Uuid, String), AppError> {
    let key = req
        .headers()
        .get(API_KEY_HEADER)
        .and_then(|k| Uuid::try_parse_ascii(k.as_bytes()).ok())
        .filter(|u| u.get_version_num() == 7)
        .ok_or(ServiceHttpError::Forbidden)?;

    let h_signature = req.headers().get(API_PAYLOAD_SIGNATURE_HEADER);

    let signature = h_signature
        .and_then(|v| BASE64_STANDARD.decode(v.as_bytes()).ok())
        .and_then(|v| Signature::from_slice(&v).ok())
        .ok_or(ServiceHttpError::Forbidden)?;

    //@todo cache this answer
    let auth_req = HttpMethod::GetKeyById { id: key };
    let HttpPayload::GetKeyById {
        public_key,
        enabled,
        ..
    } = internal_http_request(url, auth_req).await?.try_into()?;

    if !enabled {
        return Err(ServiceHttpError::Forbidden.into());
    }

    let public_key_der = BASE64_STANDARD
        .decode(public_key)
        .map_err(|_| ServiceHttpError::Forbidden)?;

    let verifying_key = VerifyingKey::from_public_key_der(&public_key_der).map_err(|_| ServiceHttpError::Forbidden)?;

    verifying_key
        .verify(body.as_bytes(), &signature)
        .map_err(|_| ServiceHttpError::Forbidden)?;

    // h_signature is base64 encoded string contains valid signature, so unwrap is safe
    let ret_signature = h_signature.unwrap().to_str().unwrap().into();
    Ok((key, ret_signature))
}
