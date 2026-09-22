db = db.getSiblingDB("admin");

db.createUser({
  user: "ape_dts",
  pwd: "123456",
  roles: [{ role: "root", db: "admin" }],
});
