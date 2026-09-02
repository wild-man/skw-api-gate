use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signature, Signer, SigningKey};
use skw_lib_shared::prelude::base64::*;

fn sign_with_pem(message: &[u8]) -> Result<Signature, ed25519_dalek::pkcs8::Error> {
    let signing_key = SigningKey::from_pkcs8_pem(
        "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIBciTyz1f9ELrN3rZ+tcxxvQa14krR0sxY6HTJOLcWbK\n-----END PRIVATE KEY-----\n",
    )?;
    Ok(signing_key.sign(message))
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let signature = sign_with_pem(r#"{"Ts":"2026-08-25T12:49:50.264895652Z", "Method":"Ping", "Params":{}}"#.as_bytes()).expect("can't sign");
    let sig_b64 = BASE64_STANDARD.encode(signature.to_bytes());
    println!("{sig_b64}");

    Ok(())
}
