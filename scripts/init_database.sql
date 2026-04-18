/*
============================================================================================
CREAING DTABASE AND SCHEMAS
===========================================================================================

Script purpose - This script creates a databse named 'datawarehouse' , Additonally it check if database with same name exists
then drops it and recreates it , besides this  also creates schemas for all the layers  within database named bronze , siler and gold.

The below script also handles any active connection to database , as if not handled properly it will not drop the database
*/


-- CREATING A FRESH DATABASE
USE master;

Select * from sys.databases
-- Remove if same name already exits
IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'datawarehouse')
BEGIN
-- we can not drop database if someone connected , hence rollback connection first
	ALTER DATABASE datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE -- sets database to single user , single connection at time 
	DROP DATABASE datawarehouse
END;
GO

-- create database
CREATE DATABASE datawarehouse;
GO

-- use this datawarehouse
USE datawarehouse;
GO

-- creating a schema for each layer
-- Schema - Its like a folder / container that keeps things orgaize
CREATE SCHEMA Bronze;
GO  -- all together statements won't work , here GO acts as separator
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO
