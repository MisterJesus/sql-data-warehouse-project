### Overview
----
The Gold Layer is the business-level representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact table** for specific metrics.

----

#### 1. gold.dim_customers 
- Purpose: Stores customer details enriched with demographic and geographic data 
- Columns:
  | Column Name | Data Type | Description |
  |-------------|-----------|-------------|
  |customer_key | INT | Surrogate Key uniquely identifying each customer record in the dimension table |
  |customer_id | INT | Unique numerical identifier assigned to each customer |
  |customer_number | NVARCHAR(50) | Alphanumeric identifier representing the customer, used for tracking and referencing |
  |first_name | NVARCHAR(50) | The customer's first name  |
  |last_name | NVARCHAR(50) | The customer's last name |
  |country | NVARCHAR(50) | The country of residence of the customer |
  |marital_status | NVARCHAR(50) | The marital status of the customer |
  |gender | NVARCHAR(50) | The gender of the customer |
  |birthdate | DATE | The birth date of the customer |
  |-------------|-----------|-------------|

----

#### 2. gold.dim_products
- Purpose: -
- Columns:
  | Column Name | Data Type | Description |
  |-------------|-----------|-------------|
  |customer_key | INT | Surrogate Key uniquely identifying each customer record in the dimension table |
  |customer_id | INT | Unique numerical identifier assigned to each customer |
  |customer_number | NVARCHAR(50) | Alphanumeric identifier representing the customer, used for tracking and referencing |
  |first_name | NVARCHAR(50) | The customer's first name  |
  |last_name | NVARCHAR(50) | The customer's last name |
  |country | NVARCHAR(50) | The country of residence of the customer |
  |marital_status | NVARCHAR(50) | The marital status of the customer |
  |gender | NVARCHAR(50) | The gender of the customer |
  |birthdate | DATE | The birth date of the customer |
  |-------------|-----------|-------------|
