use std::{fmt, fs, sync::Arc};

use anyhow::{bail, Context};
use rustls::{
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        WebPkiServerVerifier,
    },
    crypto::WebPkiSupportedAlgorithms,
    pki_types::{pem::PemObject, CertificateDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};

use crate::config::ssl_config::{SslConfig, SslMode};

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

pub fn build_tls_client_config(ssl_config: &SslConfig) -> anyhow::Result<ClientConfig> {
    match &ssl_config.ssl_mode {
        SslMode::Disable => bail!("can not build a TLS client when ssl_mode=disable"),
        SslMode::Require => {
            let mut config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoCertificateVerification::default()))
                .with_no_client_auth();
            config.enable_sni = false;
            Ok(config)
        }
        SslMode::VerifyCa => {
            let verifier =
                NoHostnameVerification::new(load_root_cert_store(&ssl_config.ssl_ca_path)?)?;
            Ok(ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth())
        }
        unsupported => bail!(
            "ssl_mode={} is not supported by this TLS client",
            unsupported
        ),
    }
}

fn load_root_cert_store(path: &str) -> anyhow::Result<RootCertStore> {
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
