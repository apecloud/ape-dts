use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Lines},
    path::PathBuf,
};

use anyhow::Context;

pub struct LogReader {
    files: Vec<PathBuf>,
    file_index: usize,
    lines: Option<Lines<BufReader<File>>>,
}

impl LogReader {
    pub fn new(dir_path: &str) -> Self {
        let files = Self::list_files(dir_path)
            .with_context(|| format!("failed to list files in dir: [{}]", dir_path))
            .unwrap();
        Self {
            files,
            file_index: 0,
            lines: Option::None,
        }
    }

    pub fn nextval(&mut self) -> anyhow::Result<Option<String>> {
        if self.file_index >= self.files.len() {
            return Ok(None);
        }

        if self.lines.is_none() {
            let path = &self.files[self.file_index];
            if let Some(file_path) = path.to_str() {
                let file = File::open(file_path)
                    .with_context(|| format!("failed to open file: [{}]", file_path))?;
                self.lines = Some(BufReader::new(file).lines());
            }
        }

        if let Some(lines) = self.lines.as_mut() {
            if let Some(result) = lines.next() {
                return Ok(Some(result?));
            } else {
                self.lines = None;
                self.file_index += 1;
                return self.nextval();
            }
        }
        Ok(None)
    }

    fn list_files(dir_path: &str) -> anyhow::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(dir_path)? {
            let path = entry?.path();
            if path.is_file() {
                files.push(path);
            }
        }
        files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
        Ok(files)
    }
}
