drop database Landpro;
create database if not exists Landpro;
use Landpro;

SHOW VARIABLES LIKE "secure_file_priv";
drop table df2;
create table if not exists df1(
extTrackingID VARCHAR (255), Region int,
	year int,
	month int,
	date int,
	time int,
	weekday VARCHAR(255),
	missing int
);

create table if not exists df2(
extTrackingID VARCHAR (255),
	year int,
	month int,
	date int,
	time int,
	weekday VARCHAR(255),
     Region VARCHAR(255),
	department	VARCHAR(255),
    Answered	int,
    Inter int,
	Name VARCHAR(255)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df1.csv'  
INTO TABLE df1
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
 IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df2_1.csv'  
INTO TABLE df2
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
 IGNORE 1 LINES;
 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df2_2.csv'  
INTO TABLE df2
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
 IGNORE 1 LINES;
 
 update df2
set Answered=1
where Answered >1;

select * from df1;
select * from df1 where weekday is not null;
select count(*) from df2;


