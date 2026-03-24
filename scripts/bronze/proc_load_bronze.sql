/*
========================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================
Script Purpose:
            This stored procedure 
            - Truncates the bronze tables before loading data.
            - Uses the BULK INSERT command to load data from csv Files to bronze tables.

Parameters:
            None.
            This stored procedure does not accept any parameters or return any values.

Usage Example:
            EXEC bronze.load_bronze;
*/

EXEC bronze.load_bronze;
