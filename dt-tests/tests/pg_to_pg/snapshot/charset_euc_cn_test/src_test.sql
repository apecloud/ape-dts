INSERT INTO tb_col_euc_cn VALUES(1, 'abc');
INSERT INTO tb_col_euc_cn VALUES(2, '中文');
INSERT INTO tb_col_euc_cn VALUES(3, NULL);
-- emoj, korea NOT supported in EUC_CN
-- INSERT INTO tb_col_euc_cn VALUES(4, '😀');
-- INSERT INTO tb_col_euc_cn VALUES(5, '대한민국');

INSERT INTO bytea_pk_test (category_id, binary_id, description) VALUES('cat1', 'hello world', 'hello world in binary');
INSERT INTO bytea_pk_test (category_id, binary_id, description) VALUES('cat2', '你好世界', 'Chinese hello world in binary');