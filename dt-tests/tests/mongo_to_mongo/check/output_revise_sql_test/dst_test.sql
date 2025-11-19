use test_db_1

-- Insert partial data (missing id=03, different value for id=02)
db.users.insertOne({ "name": "Alice", "age": 30, "email": "alice@example.com", "_id": "65733a82fb2ce9836745de01" });
db.users.insertOne({ "name": "Bob", "age": 99, "email": "bob_updated@example.com", "_id": "65733a82fb2ce9836745de02" });
-- id=03 is missing (will generate miss log with insertOne command)
-- id=02 has different age and email (will generate diff log with updateOne command)
