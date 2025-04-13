DROP TABLE IF EXISTS {{params.table_name}};
CREATE TABLE {{params.table_name}} (
    id integer NOT NULL,
    user_name VARCHAR (50) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);