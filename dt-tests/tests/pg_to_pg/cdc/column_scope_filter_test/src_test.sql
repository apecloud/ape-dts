INSERT INTO column_scope.selected VALUES (1, 'first', 'private'), (2, 'second', 'private');
INSERT INTO column_scope.blocked VALUES (1, 'first', 'private'), (2, 'second', 'private');
INSERT INTO column_scope.sibling VALUES (1, 'first', 'private'), (2, 'second', 'private');
UPDATE column_scope.selected SET id=3, note='updated' WHERE id=1;
DELETE FROM column_scope.selected WHERE id=2;
INSERT INTO column_scope.a_empty VALUES (1, 'discarded', 'private');
UPDATE column_scope.a_empty SET note='ignored';
DELETE FROM column_scope.a_empty;
INSERT INTO column_scope.a_empty VALUES (2, 'still discarded', 'private');
