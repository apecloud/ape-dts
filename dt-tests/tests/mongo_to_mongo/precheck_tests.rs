#[cfg(test)]
mod test {

    use dt_precheck::meta::check_item::CheckItem;
    use serial_test::serial;
    use std::collections::{HashMap, HashSet};

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn basic_test() {
        let test_dir = "mongo_to_mongo/precheck/basic_test";
        let mut src_expected_results = HashMap::new();
        src_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);
        let mut target_expected_results = HashMap::new();
        target_expected_results.insert(CheckItem::CheckIfTableStructSupported.to_string(), false);

        TestBase::run_precheck_test(
            test_dir,
            &HashSet::new(),
            &src_expected_results,
            &target_expected_results,
        )
        .await
    }

    #[tokio::test]
    #[serial]
    async fn precheck_sharding_test() {
        let test_dir = "mongo_to_mongo/precheck/sharding_test";

        TestBase::run_precheck_test(test_dir, &HashSet::new(), &HashMap::new(), &HashMap::new())
            .await
    }
}
