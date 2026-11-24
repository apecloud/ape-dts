use mongo_snapshot_resume_src;
db.duplicate_ids.insertMany([
  { "_id": 42, "tenant": "A", "region": "east" },
  { "_id": 42, "tenant": "A", "region": "west" },
  { "_id": 43, "tenant": "B", "region": "west" }
]);
