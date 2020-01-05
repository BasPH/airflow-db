SET SCHEMA 'public';
CREATE TABLE dummy (
    testid integer,
    person varchar(50)
);
COPY dummy FROM '/docker-entrypoint-initdb.d/testdata.csv' CSV HEADER;
