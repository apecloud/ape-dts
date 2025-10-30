use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};

pub struct FileUtil {}

impl FileUtil {
    pub async fn tail(path: &str, n: usize) -> anyhow::Result<Vec<String>> {
        let mut file = File::open(path).await?;
        file.seek(SeekFrom::End(0)).await?;

        let mut cur_char: [u8; 1] = [0];
        let mut read_lines = 0;

        while n > read_lines {
            let i = match file.seek(SeekFrom::Current(-1)).await {
                Ok(i) => i,
                Err(_) => break,
            };

            if i == 0 {
                break;
            }

            if file.read(&mut cur_char).await? != 1 {
                continue;
            }

            file.seek(SeekFrom::Current(-1)).await?;
            if cur_char[0] as char == '\n' {
                read_lines += 1;
            }
        }

        let mut buf = String::new();
        file.read_to_string(&mut buf).await?;
        let lines: Vec<String> = buf.split('\n').map(|i| i.to_string()).collect();
        Ok(lines)
    }
}
