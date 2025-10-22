-- Create nodes
GRAPH.QUERY graph1 "CREATE (:Person {name: 'John', age: 30})"
GRAPH.QUERY graph1 "CREATE (:Person {name: 'Jane', age: 25})"
GRAPH.QUERY graph1 "CREATE (:Company {name: 'TechCorp', founded: 2010})"

-- Create relationships
GRAPH.QUERY graph2 "CREATE (:Person {name: 'Alice', age: 28})-[:WORKS_FOR {since: 2020}]->(:Company {name: 'DataCorp', employees: 500})"
GRAPH.QUERY graph2 "MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'DataCorp'}) CREATE (p)-[:LIVES_IN]->(:City {name: 'New York', population: 8000000})"

-- Match and return queries
GRAPH.QUERY graph3 "MATCH (p:Person) RETURN p.name, p.age"
GRAPH.QUERY graph3 "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name"

-- Update node properties
GRAPH.QUERY graph4 "CREATE (:Employee {id: 1, name: 'Bob', salary: 50000})"
GRAPH.QUERY graph4 "MATCH (e:Employee {id: 1}) SET e.salary = 55000"
GRAPH.QUERY graph4 "MATCH (e:Employee {id: 1}) SET e.department = 'Engineering'"

-- Delete operations
GRAPH.QUERY graph5 "CREATE (:TempNode {id: 'temp1'})"
GRAPH.QUERY graph5 "MATCH (t:TempNode {id: 'temp1'}) DELETE t"

-- Create with multiple nodes and relationships
GRAPH.QUERY graph6 "CREATE (p1:Person {name: 'Charlie', age: 35})-[:KNOWS {since: 2015}]->(p2:Person {name: 'David', age: 32})-[:WORKS_FOR]->(c:Company {name: 'StartupXYZ'})"

-- Complex queries with WHERE clauses
GRAPH.QUERY graph7 "CREATE (:Product {name: 'Laptop', price: 1200, category: 'Electronics'})"
GRAPH.QUERY graph7 "CREATE (:Product {name: 'Book', price: 25, category: 'Education'})"
GRAPH.QUERY graph7 "MATCH (p:Product) WHERE p.price > 100 RETURN p.name, p.price"

-- Aggregation queries
GRAPH.QUERY graph8 "CREATE (:Order {id: 1, amount: 150, date: '2023-01-15'})"
GRAPH.QUERY graph8 "CREATE (:Order {id: 2, amount: 300, date: '2023-01-16'})"
GRAPH.QUERY graph8 "MATCH (o:Order) RETURN count(o), sum(o.amount), avg(o.amount)"

-- Path queries
GRAPH.QUERY graph9 "CREATE (a:Airport {code: 'JFK', city: 'New York'})-[:FLIGHT {duration: 360}]->(b:Airport {code: 'LAX', city: 'Los Angeles'})"
GRAPH.QUERY graph9 "CREATE (b:Airport {code: 'LAX'})-[:FLIGHT {duration: 240}]->(c:Airport {code: 'SFO', city: 'San Francisco'})"
GRAPH.QUERY graph9 "MATCH path = (a:Airport {code: 'JFK'})-[:FLIGHT*1..2]->(c:Airport) RETURN path"

-- Update relationships
GRAPH.QUERY graph10 "CREATE (p:Person {name: 'Eve'})-[r:FRIEND_OF {since: 2020}]->(f:Person {name: 'Frank'})"
GRAPH.QUERY graph10 "MATCH (p:Person {name: 'Eve'})-[r:FRIEND_OF]->(f:Person {name: 'Frank'}) SET r.closeness = 'high'"

-- Conditional updates
GRAPH.QUERY graph11 "CREATE (:User {id: 1, status: 'active', last_login: '2023-01-01'})"
GRAPH.QUERY graph11 "MATCH (u:User {id: 1}) SET u.status = CASE WHEN u.last_login < '2023-06-01' THEN 'inactive' ELSE 'active' END"

-- Multiple labels
GRAPH.QUERY graph12 "CREATE (:Person:Employee {name: 'Grace', id: 123, department: 'HR'})"
GRAPH.QUERY graph12 "MATCH (pe:Person:Employee) RETURN pe.name, pe.department"

-- Index operations (if supported)
GRAPH.QUERY graph13 "CREATE (:Customer {email: 'john@example.com', name: 'John Doe'})"
GRAPH.QUERY graph13 "CREATE (:Customer {email: 'jane@example.com', name: 'Jane Smith'})"

-- Complex relationship patterns
GRAPH.QUERY graph14 "CREATE (m:Manager {name: 'Sarah'})-[:MANAGES]->(e1:Employee {name: 'Tom'}), (m)-[:MANAGES]->(e2:Employee {name: 'Lisa'})"
GRAPH.QUERY graph14 "MATCH (m:Manager)-[:MANAGES]->(e:Employee) RETURN m.name, collect(e.name)"

-- Optional match
GRAPH.QUERY graph15 "CREATE (:Person {name: 'Alex'})"
GRAPH.QUERY graph15 "CREATE (:Person {name: 'Beth'})-[:HAS_PHONE]->(:Phone {number: '555-0123'})"
GRAPH.QUERY graph15 "MATCH (p:Person) OPTIONAL MATCH (p)-[:HAS_PHONE]->(phone:Phone) RETURN p.name, phone.number"

-- Union queries
GRAPH.QUERY graph16 "CREATE (:Student {name: 'Mike', grade: 'A'})"
GRAPH.QUERY graph16 "CREATE (:Teacher {name: 'Prof. Wilson', subject: 'Math'})"
GRAPH.QUERY graph16 "MATCH (s:Student) RETURN s.name AS name, 'Student' AS type UNION MATCH (t:Teacher) RETURN t.name AS name, 'Teacher' AS type"

-- Delete with relationships
GRAPH.QUERY graph17 "CREATE (p:Person {name: 'DeleteMe'})-[:OWNS]->(c:Car {model: 'Toyota'})"
GRAPH.QUERY graph17 "MATCH (p:Person {name: 'DeleteMe'})-[r:OWNS]->(c:Car) DELETE r, p, c"

-- Bulk operations
GRAPH.QUERY graph18 "UNWIND range(1, 5) AS i CREATE (:Number {value: i})"
GRAPH.QUERY graph18 "MATCH (n:Number) WHERE n.value % 2 = 0 SET n.type = 'even'"
GRAPH.QUERY graph18 "MATCH (n:Number) WHERE n.value % 2 = 1 SET n.type = 'odd'"
