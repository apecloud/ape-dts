INSERT INTO [ape_dts].[dbo].[snapshot_check] (tenant, id, val) VALUES (1, 1, NULL), (1, 2, 99);

INSERT INTO [ape_dts].[dbo].[sc_data_one_pk_multi_uk] (id, code, external_id, payload) VALUES
    (1, 10, 501, N'上海'),
    (2, 20, 502, NULL),
    (3, 30, 503, N'old');

INSERT INTO [ape_dts].[dbo].[sc_data_no_pk_composite_uk] (tenant, code, qty) VALUES
    (1, 10, 3),
    (2, 10, 40);

INSERT INTO [ape_dts].[dbo].[sc_data_no_pk_multi_uk] (seq, external_code, amount) VALUES
    (10, N'AA', 10);

INSERT INTO [ape_dts].[dbo].[sc_data_no_pk_no_uk] (category, label) VALUES
    (1, N'first');

INSERT INTO [ape_dts].[dbo].[sc_data_quoted_keys] ([order id], [line:no], [display name]) VALUES
    (1, 1, N'甲'),
    (1, 2, N'丙');

INSERT INTO [ape_dts].[dbo].[sc_data_typed_values] (id, amount, observed_at, event_date, token, payload, active) VALUES
    (1, -1234.5000, '2024-02-29T23:59:59.123', '2024-02-29', '9f3257dd-f019-4ad6-a22b-ef208aca1872', 0x0001FF, 1),
    (2, NULL, NULL, NULL, NULL, NULL, 0);

INSERT INTO [ape_dts].[dbo].[sc_data_calculated] (id, qty) VALUES
    (1, 3),
    (2, NULL);
