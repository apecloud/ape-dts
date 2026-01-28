#[cfg(test)]
mod test {

    use dt_connector::checker::log_reader::LogReader;
    use serial_test::serial;

    use crate::test_config_util::TestConfigUtil;

    #[test]
    #[serial]
    fn log_reader_empty_test() {
        let dir = TestConfigUtil::get_absolute_path("log_reader/log_reader_empty_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.nextval().unwrap(), None);
    }

    #[test]
    #[serial]
    fn log_reader_one_log_test() {
        let dir = TestConfigUtil::get_absolute_path("log_reader/log_reader_one_log_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.nextval().unwrap().unwrap(), "log1.line1");
        assert_eq!(reader.nextval().unwrap().unwrap(), "");
        assert_eq!(reader.nextval().unwrap().unwrap(), "log1.line3");
        assert_eq!(reader.nextval().unwrap(), None);
    }

    #[test]
    #[serial]
    fn log_reader_multi_log_test() {
        let dir = TestConfigUtil::get_absolute_path("log_reader/log_reader_multi_log_test");
        let mut reader = LogReader::new(&dir);
        assert_eq!(reader.nextval().unwrap().unwrap(), "log1.line1");
        assert_eq!(reader.nextval().unwrap().unwrap(), "");
        assert_eq!(reader.nextval().unwrap().unwrap(), "log1.line3");
        assert_eq!(reader.nextval().unwrap().unwrap(), "");

        assert_eq!(reader.nextval().unwrap().unwrap(), "log2.line2");
        assert_eq!(reader.nextval().unwrap().unwrap(), "");
        assert_eq!(reader.nextval().unwrap().unwrap(), "log2.line4");
        assert_eq!(reader.nextval().unwrap().unwrap(), "");

        assert_eq!(reader.nextval().unwrap().unwrap(), "");
        assert_eq!(reader.nextval().unwrap(), None);
    }
}
