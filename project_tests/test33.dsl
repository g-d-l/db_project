-- Needs test08.dsl,test29.dsl and test32.dsl to have been executed first.
-- Correctness test: Delete values and run a simple query
-- 
-- DELETE FROM tbl2 WHERE col1 = -10;
-- DELETE FROM tbl2 WHERE col2 = -22;
-- DELETE FROM tbl2 WHERE col1 = -30;
-- DELETE FROM tbl2 WHERE col3 = -444;
-- DELETE FROM tbl2 WHERE col1 = -50;
--
-- SELECT col1 FROM tbl2 WHERE col7 > -100 AND col7 < 100000000;
--
s1=select(db1.tbl2.col1,-10,-10)
relational_delete(db1.tbl2,s1)
s2=select(db1.tbl2.col2,-22,-22)
relational_delete(db1.tbl2,s2)
s3=select(db1.tbl2.col1,-30,-30)
relational_delete(db1.tbl2,s3)
s4=select(db1.tbl2.col3,-444,-444)
relational_delete(db1.tbl2,s4)
s5=select(db1.tbl2.col1,-50,-50)
relational_delete(db1.tbl2,s5)
s6=select(db1.tbl2.col7,-99,99999999)
f1=fetch(db1.tbl2.col1,s6)
tuple(f1)
