-- Keep PK/UK values unchanged so CDC updates use the merge DELETE/INSERT path.
-- Only ordinary source DML is used; the destination has no competing test writer.
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-1-', id, '-', REPEAT('x', 128));
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-2-', id, '-', REPEAT('x', 128));
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-3-', id, '-', REPEAT('x', 128));
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-4-', id, '-', REPEAT('x', 128));
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-5-', id, '-', REPEAT('x', 128));
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-6-', id, '-', REPEAT('x', 128));
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-7-', id, '-', REPEAT('x', 128));
UPDATE cdc_deadlock.accounts SET bucket=MOD(bucket+1, 8), revision=revision+1, payload=CONCAT('round-8-', id, '-', REPEAT('x', 128));
