create database test_chinese_short;use test_chinese_short;
create table test_chinese_short.output (ts timestamp, str1 nchar(10),str2 nchar(10)) tags (name1 nchar(10),name2 nchar(10));
insert into test_chinese_short.o using test_chinese_short.output tags('一二三四五六七八九十','一二三四五六七八九十') values (now,'一二三四五六七八九十','一二三四五六七八九十');
