drop database if exists db;
create database if not exists db;
create stable db.stbxx (ts timestamp, c1 int, c2 bool, c3 tinyint, c4 float) tags(t1 int, t2 double);
insert into db.tbxx using db.stbxx tags(1, 1.0) values(1500000000000, 1, true, 10, 20.0);
insert into db.`子表1` using db.stbxx tags(2, 2.0) values(1500000000000, 2, false, 20, 30.0);

create stable db.`超级表1` (ts timestamp, cc1 float, cc2 double, cc3 binary(10), cc4 nchar(10), cc5 timestamp) tags(tt1 float, tt2 int, tt3 binary(10));
insert into db.tbyy using db.`超级表1` tags(1.0, "你好") values(1500000000000, 1.0, 30.0, "China", "世界");
insert into db.`超级表1的子表` using db.`超级表1` tags(2.0, "Beijing") values(1500000000000, 2.0, 100.0, "USA", "纽约");

create table db.normaltb (ts timestamp, nc1 int);
insert into db.normaltb values(1500000000000, 3);

create table db.`就是一个普通表` (ts timestamp, nc1 float);
insert into db.`就是一个普通表` values(1500000000000, 3.0);
