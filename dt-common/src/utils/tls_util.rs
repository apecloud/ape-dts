use std::{fmt, fs, sync::Arc};

use anyhow::{bail, Context};
use rustls::{
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        WebPkiServerVerifier,
    },
    crypto::WebPkiSupportedAlgorithms,
    pki_types::{pem::PemObject, CertificateDer, ServerName, UnixTime},
    DigitallySignedStruct, RootCertStore, SignatureScheme,
};

pub struct NoCertificateVerification {
    supported: WebPkiSupportedAlgorithms,
}

impl Default for NoCertificateVerification {
    fn default() -> Self {
        Self {
            supported: rustls::crypto::ring::default_provider().signature_verification_algorithms,
        }
    }
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

impl fmt::Debug for NoCertificateVerification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoCertificateVerification").finish()
    }
}

#[derive(Debug)]
pub struct NoHostnameVerification {
    inner: Arc<WebPkiServerVerifier>,
}

impl NoHostnameVerification {
    pub fn new(root_store: RootCertStore) -> anyhow::Result<Self> {
        let inner = WebPkiServerVerifier::builder(Arc::new(root_store))
            .build()
            .context("failed to build TLS server certificate verifier")?;
        Ok(Self { inner })
    }
}

impl ServerCertVerifier for NoHostnameVerification {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
            .or_else(|error| {
                if matches!(
                    error,
                    rustls::Error::InvalidCertificate(
                        rustls::CertificateError::NotValidForName
                            | rustls::CertificateError::NotValidForNameContext { .. }
                    )
                ) {
                    Ok(ServerCertVerified::assertion())
                } else {
                    Err(error)
                }
            })
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

pub(crate) fn load_root_cert_store(path: &str) -> anyhow::Result<RootCertStore> {
    if path.is_empty() {
        bail!("TLS CA certificate path is empty")
    }

    let pem =
        fs::read(path).with_context(|| format!("failed to read TLS CA certificate: {}", path))?;
    let certificates = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse TLS CA certificate: {}", path))?;
    if certificates.is_empty() {
        bail!("TLS CA certificate file contains no certificates: {}", path)
    }

    let mut root_store = RootCertStore::empty();
    for certificate in certificates {
        root_store
            .add(certificate)
            .with_context(|| format!("failed to add TLS CA certificate: {}", path))?;
    }
    Ok(root_store)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::ssl_config::{SslConfig, SslMode};

    #[test]
    fn verified_modes_reject_invalid_certificates_and_hostnames() {
        let certificate = CertificateDer::from_pem_slice(include_bytes!(
            "../../../dt-tests/docker/tls/server/server.crt"
        ))
        .unwrap();
        let ca_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../dt-tests/docker/tls/server/server-ca.crt"
        );
        let now = UnixTime::now();
        // Keep the DER structure intact while invalidating the certificate signature.
        let mut corrupted = certificate.to_vec();
        *corrupted.last_mut().unwrap() ^= 1;
        let corrupted = CertificateDer::from(corrupted);
        let expired = UnixTime::since_unix_epoch(Duration::from_secs(
            now.as_secs() + 20 * 365 * 24 * 60 * 60,
        ));
        let verifier =
            WebPkiServerVerifier::builder(Arc::new(load_root_cert_store(ca_path).unwrap()))
                .build()
                .unwrap();
        let relaxed = NoHostnameVerification::new(load_root_cert_store(ca_path).unwrap()).unwrap();
        for (name, cert, host, time, accepted, accepted_without_hostname) in [
            ("valid", &certificate, "localhost", now, true, true),
            (
                "hostname mismatch",
                &certificate,
                "different.example",
                now,
                false,
                true,
            ),
            ("IP mismatch", &certificate, "127.0.0.1", now, false, true),
            (
                "invalid signature",
                &corrupted,
                "localhost",
                now,
                false,
                false,
            ),
            ("expired", &certificate, "localhost", expired, false, false),
        ] {
            let result = verifier.verify_server_cert(
                cert,
                &[],
                &ServerName::try_from(host).unwrap(),
                &[],
                time,
            );
            assert_eq!(result.is_ok(), accepted, "{name}: {result:?}");
            let result = relaxed.verify_server_cert(
                cert,
                &[],
                &ServerName::try_from(host).unwrap(),
                &[],
                time,
            );
            assert_eq!(
                result.is_ok(),
                accepted_without_hostname,
                "verify_ca {name}: {result:?}"
            );
        }
        for mode in [SslMode::VerifyCa, SslMode::VerifyFull] {
            let config = SslConfig {
                ssl_mode: mode,
                ssl_ca_path: ca_path.into(),
                ..SslConfig::default()
            }
            .to_rustls_client_config()
            .unwrap();
            assert!(config.enable_sni);
        }
    }
}
