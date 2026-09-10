INSERT INTO order_key_src.mixed VALUES (1,3,1,'a'),(1,3,2,'b'),(1,2,1,'c'),(1,2,2,'d'),(1,1,1,'e'),(2,3,1,'f'),(2,3,2,'g'),(2,2,1,'h'),(2,1,1,'i'),(3,1,1,'j');
INSERT INTO order_key_src.descending VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g');
INSERT INTO order_key_src.selected VALUES ('z',1,'a'),('y',2,'b'),('x',3,'c'),('w',4,'d'),('v',5,'e'),('u',6,'f'),('t',7,'g');
INSERT INTO order_key_src.scored_key SELECT * FROM order_key_src.selected;
INSERT INTO order_key_src.nullable_key VALUES (3,1,'a'),(3,2,'b'),(2,1,'c'),(2,2,'d'),(1,1,'e'),(1,2,'f'),(NULL,1,'g'),(1,NULL,'h'),(NULL,NULL,'i');
INSERT INTO order_key_src.float_key VALUES (-3.5,'a'),(-1.25,'b'),(0,'c'),(1.25,'d'),(2.5,'e'),(7.75,'f'),(12.5,'g');
INSERT INTO order_key_src.catalog_key VALUES ('z',1,3,'a'),('y',1,2,'b'),('x',1,1,'c'),('w',2,3,'d'),('v',2,2,'e'),('u',2,1,'f'),('t',3,1,'g');

INSERT INTO order_key_src.cursor_ignored VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g');

INSERT INTO order_key_src.network_key VALUES ('10.0.0.1','a'),('10.0.0.2','b'),('10.0.0.3','c'),('10.0.0.4','d'),('10.0.0.5','e'),('10.0.0.6','f'),('10.0.0.7','g');
INSERT INTO order_key_src.range_key VALUES ('[1,2)','a'),('[2,3)','b'),('[3,4)','c'),('[4,5)','d'),('[5,6)','e'),('[6,7)','f'),('[7,8)','g');
INSERT INTO order_key_src.array_key VALUES ('{1,1}','a'),('{2,1}','b'),('{3,1}','c'),('{4,1}','d'),('{5,1}','e'),('{6,1}','f'),('{7,1}','g');
INSERT INTO order_key_src.bool_key VALUES (false,'a'),(true,'b');
