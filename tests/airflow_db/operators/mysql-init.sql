CREATE TABLE dummy (
    testid integer,
    person varchar(50)
);
LOAD DATA INFILE '/docker-entrypoint-initdb.d/testdata.csv' INTO TABLE dummy FIELDS TERMINATED BY ',' IGNORE 1 LINES;
