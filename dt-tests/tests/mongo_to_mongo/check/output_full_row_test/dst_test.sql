use test_db_1

-- Insert partial data in destination (missing _id ending in 013, different value for _id ending in 012)
db.users.insertMany([
    { _id: ObjectId("507f1f77bcf86cd799439011"), name: "Alice", age: 30, email: "alice@example.com" },
    { _id: ObjectId("507f1f77bcf86cd799439012"), name: "Bob", age: 99, email: "bob_updated@example.com" }
]);
-- _id ending in 013 is missing (will generate miss log)
-- _id ending in 012 has different age and email (will generate diff log)
