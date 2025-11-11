INSERT INTO resume_table_1(pk, val) VALUES (1, 30);
INSERT INTO resume_table_1(pk, val) VALUES (2, 30);

INSERT INTO resume_table_2("p.k", val) VALUES (1, 30);
INSERT INTO resume_table_2("p.k", val) VALUES (2, 30);

INSERT INTO resume_table_3(f_0, f_1) VALUES (1, 30);
INSERT INTO resume_table_3(f_0, f_1) VALUES (2, 30);

INSERT INTO "resume_table_*$4"("p.k", val) VALUES (1, 30),(2, 30);

INSERT INTO nullable_composite_unique_key_table VALUES(1, '1', 1),(2, '2', 2),(3, '3', 3),(4, '4', 4),(5, '5', 5),(6, '6', 6),(7, NULL, 7),(NULL, '8', 8),(NULL, 'null', 9),(NULL, NULL, 10),(NULL, NULL, NULL),(NULL, NULL, NULL);

INSERT INTO "test_db_*.*"."resume_table_*$5"("p.k", val) VALUES (1, 30),(2, 30);

INSERT INTO "test_db_*.*"."finished_table_*$1"("p.k", val) VALUES (1, 30),(2,30);

INSERT INTO "test_db_*.*"."finished_table_*$2"("p.k", val) VALUES (1, 30),(2,30);

INSERT INTO "test_db_*.*"."in_finished_log_table_*$1"("p.k", val) VALUES (1, 30),(2,30);

INSERT INTO "test_db_*.*"."in_finished_log_table_*$2"("p.k", val) VALUES (1, 30),(2,30);

INSERT INTO "test_db_*.*"."in_position_log_table_*$1"("p.k", val) VALUES (1, 30),(2,30);