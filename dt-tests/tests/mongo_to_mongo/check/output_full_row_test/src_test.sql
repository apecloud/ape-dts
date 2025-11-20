use test_db_1

db.users.insertMany([
    { _id: ObjectId("507f1f77bcf86cd799439011"), name: "Alice", age: 30, email: "alice@example.com" },
    { _id: ObjectId("507f1f77bcf86cd799439012"), name: "Bob", age: 25, email: "bob@example.com" },
    { _id: ObjectId("507f1f77bcf86cd799439013"), name: "Charlie", age: 35, email: "charlie@example.com" }
]);
