use mongo_tls_snapshot

db.items.insertMany([
  { "_id": 1, "name": "first", "enabled": true },
  { "_id": 2, "name": "second", "enabled": false },
  { "_id": 3, "name": "third", "enabled": true }
]);
