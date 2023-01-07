CREATE DATABASE IF NOT EXISTS conta;

USE conta;

CREATE TABLE IF NOT EXISTS ContaBancaria (
    id INT NOT NULL,
    number_conta INT NOT NULL,
    value FLOAT NOT NULL,
    PRIMARY KEY(id)
);
