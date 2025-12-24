/*
CREATE THE DATBASE AND SCHEMA

OBJECTIVE:
THE SCRIPT IS RESPONSIBLE FOR DROPPING AND THEN RECREATING DATABASE 'DataWarehouse'.
Next three schemas are created : 'BRONZE', 'SILVER', 'GOLD'


WARNING:
THIS SCRIPT CLEARS THE DATABASE BEFORE RECREATING

*/



USE master;

-- Drop the Database if exists
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	DROP DATABASE DataWarehouse;
END;
GO

-- Create Database

CREATE DATABASE DataWarehouse;
GO

-- Switch to the Database
USE DataWarehouse;
GO

-- Create Schema

create schema bronze;
go
create schema silver;
go
create schema gold;
go
