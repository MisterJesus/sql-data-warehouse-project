-- Chapter 12: Project 1 - Data Warehouse --

/* 
========================================================================
Create Database and Schemas
========================================================================
Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists If the database exists, 
  it is dropped and recreated. Additionally, the script sets up three schema within the database: 'bronze', 'silver', and 'gold'

WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it exists. 
  All data in the database will be permanently deleted. 
  Proceed with cautior and ensure you have proper backups before running this script.
*/

USE master;
GO                    -- The clause GO helps you execute multiple statements that are ending with semicolon (;)

-- Drop and Recreate the 'DataWarehouse' database. For when you forgot if the database already existed --
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END;
GO

-- Create Database --
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

-- Create Schemas --
CREATE SCHEMA bronze;	-- To check Schema: Server > Databases > DataWarehouse > Security > Schemas > bronze
GO						
CREATE SCHEMA silver;	-- For the silver layer part of the project --
GO
CREATE SCHEMA gold;		-- For the gold layer part of the project --
