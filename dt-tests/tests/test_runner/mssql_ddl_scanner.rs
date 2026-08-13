pub(super) fn extract_created_tables(batches: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    let mut db_tbs = Vec::new();
    for batch in batches {
        let mut scanner = MssqlDdlScanner::new(batch);
        while scanner.find_keyword("create")? {
            if !scanner.consume_keyword("table")? {
                continue;
            }

            let first = scanner
                .read_identifier()?
                .ok_or_else(|| anyhow::anyhow!("missing table name after MSSQL CREATE TABLE"))?;
            let mut identifiers = vec![first];
            while scanner.consume_dot()? {
                let identifier = scanner.read_identifier()?.ok_or_else(|| {
                    anyhow::anyhow!("missing identifier after '.' in MSSQL CREATE TABLE name")
                })?;
                identifiers.push(identifier);
            }

            let table = identifiers.pop().unwrap();
            // Local and global temporary tables are connection-scoped and are not snapshot
            // migration targets.
            if !table.starts_with('#') {
                let schema = identifiers.pop().unwrap_or_else(|| "dbo".to_string());
                db_tbs.push((schema, table));
            }
        }
    }
    Ok(db_tbs)
}

struct MssqlDdlScanner<'a> {
    sql: &'a str,
    index: usize,
}

impl<'a> MssqlDdlScanner<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, index: 0 }
    }

    fn find_keyword(&mut self, keyword: &str) -> anyhow::Result<bool> {
        while self.index < self.sql.len() {
            self.skip_trivia()?;
            if self.index >= self.sql.len() {
                return Ok(false);
            }

            match self.current_byte() {
                b'\'' => self.skip_string_literal()?,
                b'[' => {
                    self.read_quoted_identifier(b'[', b']')?;
                }
                b'"' => {
                    self.read_quoted_identifier(b'"', b'"')?;
                }
                _ => {
                    if let Some(word) = self.read_bare_word() {
                        if word.eq_ignore_ascii_case(keyword) {
                            return Ok(true);
                        }
                    } else {
                        self.advance_char();
                    }
                }
            }
        }
        Ok(false)
    }

    fn consume_keyword(&mut self, keyword: &str) -> anyhow::Result<bool> {
        self.skip_trivia()?;
        let Some(word) = self.read_bare_word() else {
            return Ok(false);
        };
        Ok(word.eq_ignore_ascii_case(keyword))
    }

    fn read_identifier(&mut self) -> anyhow::Result<Option<String>> {
        self.skip_trivia()?;
        if self.index >= self.sql.len() {
            return Ok(None);
        }

        match self.current_byte() {
            b'[' => self.read_quoted_identifier(b'[', b']').map(Some),
            b'"' => self.read_quoted_identifier(b'"', b'"').map(Some),
            _ => Ok(self.read_bare_word().map(str::to_string)),
        }
    }

    fn consume_dot(&mut self) -> anyhow::Result<bool> {
        self.skip_trivia()?;
        if self.index < self.sql.len() && self.current_byte() == b'.' {
            self.index += 1;
            return Ok(true);
        }
        Ok(false)
    }

    fn skip_trivia(&mut self) -> anyhow::Result<()> {
        loop {
            while self.index < self.sql.len() && self.current_byte().is_ascii_whitespace() {
                self.index += 1;
            }

            if self.remaining().starts_with("--") {
                self.index += 2;
                while self.index < self.sql.len() && self.current_byte() != b'\n' {
                    self.advance_char();
                }
                continue;
            }

            if self.remaining().starts_with("/*") {
                self.skip_block_comment()?;
                continue;
            }

            return Ok(());
        }
    }

    fn skip_block_comment(&mut self) -> anyhow::Result<()> {
        self.index += 2;
        let mut depth = 1;
        while self.index < self.sql.len() && depth > 0 {
            if self.remaining().starts_with("/*") {
                depth += 1;
                self.index += 2;
            } else if self.remaining().starts_with("*/") {
                depth -= 1;
                self.index += 2;
            } else {
                self.advance_char();
            }
        }
        anyhow::ensure!(depth == 0, "unterminated block comment in MSSQL test SQL");
        Ok(())
    }

    fn skip_string_literal(&mut self) -> anyhow::Result<()> {
        self.index += 1;
        loop {
            anyhow::ensure!(
                self.index < self.sql.len(),
                "unterminated string literal in MSSQL test SQL"
            );
            if self.current_byte() == b'\'' {
                if self.remaining().starts_with("''") {
                    self.index += 2;
                } else {
                    self.index += 1;
                    return Ok(());
                }
            } else {
                self.advance_char();
            }
        }
    }

    fn read_quoted_identifier(&mut self, open: u8, close: u8) -> anyhow::Result<String> {
        debug_assert_eq!(self.current_byte(), open);
        self.index += 1;
        let mut identifier = String::new();
        loop {
            anyhow::ensure!(
                self.index < self.sql.len(),
                if open == b'[' {
                    "unterminated bracket identifier in MSSQL test SQL"
                } else {
                    "unterminated quoted identifier in MSSQL test SQL"
                }
            );

            if self.current_byte() == close {
                if self.sql.as_bytes().get(self.index + 1) == Some(&close) {
                    identifier.push(close as char);
                    self.index += 2;
                } else {
                    self.index += 1;
                    return Ok(identifier);
                }
            } else {
                let ch = self.current_char();
                identifier.push(ch);
                self.index += ch.len_utf8();
            }
        }
    }

    fn read_bare_word(&mut self) -> Option<&'a str> {
        if self.index >= self.sql.len() || !Self::is_bare_word_char(self.current_char()) {
            return None;
        }

        let start = self.index;
        self.advance_char();
        while self.index < self.sql.len() && Self::is_bare_word_char(self.current_char()) {
            self.advance_char();
        }
        Some(&self.sql[start..self.index])
    }

    fn is_bare_word_char(ch: char) -> bool {
        ch.is_alphanumeric() || matches!(ch, '_' | '@' | '#' | '$')
    }

    fn remaining(&self) -> &'a str {
        &self.sql[self.index..]
    }

    fn current_byte(&self) -> u8 {
        self.sql.as_bytes()[self.index]
    }

    fn current_char(&self) -> char {
        self.remaining().chars().next().unwrap()
    }

    fn advance_char(&mut self) {
        self.index += self.current_char().len_utf8();
    }
}

#[cfg(test)]
mod tests {
    use super::extract_created_tables;

    #[test]
    fn extracts_created_tables_from_batches() {
        let batches = vec![
            r#"
                -- CREATE TABLE ignored.comment_table (id int);
                DROP TABLE IF EXISTS dbo.orders;
                CREATE /* comment */ TABLE dbo.orders (id int);
                CREATE TABLE [schema.with.dot].[table with space] (id int);
                SELECT 'CREATE TABLE ignored.string_table (id int)';
                /* nested /* CREATE TABLE ignored.block_table (id int) */ comment */
                CREATE TABLE [escaped]]schema].[escaped]]table] (id int);
                [CREATE] TABLE ignored.quoted_keyword (id int);
                CREATE [TABLE] ignored.quoted_table_keyword (id int);
                CREATE VIEW dbo.order_view AS SELECT 1 AS id;
                CREATE TABLE "quoted""schema"."quoted""table" (id int);
            "#
            .to_string(),
            r#"
                CREATE TABLE default_schema_table (id int);
                CREATE TABLE database_name.audit.events (id int);
                CREATE TABLE #local_temp (id int);
                CREATE TABLE ##global_temp (id int);
            "#
            .to_string(),
        ];

        let db_tbs = extract_created_tables(&batches).unwrap();
        assert_eq!(
            db_tbs,
            vec![
                ("dbo".to_string(), "orders".to_string()),
                (
                    "schema.with.dot".to_string(),
                    "table with space".to_string()
                ),
                ("escaped]schema".to_string(), "escaped]table".to_string()),
                ("quoted\"schema".to_string(), "quoted\"table".to_string()),
                ("dbo".to_string(), "default_schema_table".to_string()),
                ("audit".to_string(), "events".to_string()),
            ]
        );
    }

    #[test]
    fn rejects_malformed_sql() {
        let batches = vec!["CREATE TABLE [unterminated (id int);".to_string()];
        let error = extract_created_tables(&batches).unwrap_err();
        assert!(error
            .to_string()
            .contains("unterminated bracket identifier"));
    }
}
