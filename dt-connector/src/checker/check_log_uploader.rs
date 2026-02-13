use anyhow::Context;
use opendal::Operator;

pub struct CheckLogUploader {
    pub s3_client: Operator,
    pub key_prefix: String,
}

impl CheckLogUploader {
    pub async fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.s3_client
            .write(key, data.to_vec())
            .await
            .with_context(|| format!("failed to upload check log: {key}"))?;
        Ok(())
    }
}
