-- Databricks notebook source
-- MAGIC %md 
-- MAGIC SQL Practice:-

-- COMMAND ----------

create database CompanyDB;

-- COMMAND ----------

create table employees(
  id INT,
  name varchar(100),
  age INT,
  department VARCHAR(100),
  salary DECIMAL(10, 2)
);

-- COMMAND ----------

SELECT * FROM  EMPLOYEES;

-- COMMAND ----------

INSERT INTO employees (id, name, age, department, salary) VALUES
(1, 'Alice', 30, 'Engineering', 70000.00),
(2, 'Bob', 34, 'Engineering', 80000.00),
(3, 'Charlie', 29, 'HR', 50000.00),
(4, 'David', 40, 'HR', 60000.00),
(5, 'Eve', 45, 'Finance', 90000.00);

-- COMMAND ----------

CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10, 2)
);

-- COMMAND ----------

INSERT INTO orders (order_id, customer_id, order_date, amount) VALUES
(1, 1, '2023-01-15', 150.50),
(2, 2, '2023-02-20', 200.75),
(3, 1, '2023-03-10', 300.40),
(4, 3, '2023-03-15', 120.00),
(5, 4, '2023-04-10', 350.90);

-- COMMAND ----------

CREATE TABLE customers (
    customer_id INT ,
    customer_name VARCHAR(100)
);

INSERT INTO customers (customer_id, customer_name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie'),
(4, 'David'),
(5, 'Eve');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. SELECT and WHERE Clause
-- MAGIC Question:
-- MAGIC Retrieve the names and ages of all employees who are older than 30 years.

-- COMMAND ----------

SELECT name, age
FROM employees
WHERE age > 30;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Aggregate Functions with GROUP BY
-- MAGIC Question:
-- MAGIC Find the average salary for each department.

-- COMMAND ----------

SELECT department, AVG(salary) AS average_salary
FROM employees
GROUP BY department;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. JOINs
-- MAGIC Question:
-- MAGIC Retrieve the order IDs and customer names for all orders placed.

-- COMMAND ----------

SELECT orders.order_id,customers.customer_name 
FROM customers
join orders on orders.customer_id = customers.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Subqueries
-- MAGIC Question:
-- MAGIC Find the names of employees who have a salary higher than the average salary of their department.

-- COMMAND ----------

SELECT name
FROM employees e1
WHERE salary > (
    SELECT AVG(salary)
    FROM employees e2
    WHERE e1.department = e2.department
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 4: Advanced SQL Queries
-- MAGIC 5. Window Functions
-- MAGIC Question:
-- MAGIC Retrieve the employee name, department, salary, and the rank of their salary within their department.

-- COMMAND ----------

select name,department,salary,
rank() over(partition by department order by salary desc)
from employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Handling NULL Values
-- MAGIC Question:
-- MAGIC Find the number of employees in each department. If a department has no employees, it should still be listed with a count of 0.

-- COMMAND ----------

CREATE TABLE departments (
    department_id INT,
    department_name VARCHAR(100)
);

INSERT INTO departments (department_id, department_name) VALUES
(1, 'Engineering'),
(2, 'HR'),
(3, 'Finance'),
(4, 'Marketing');

SELECT d.department_name, COUNT(e.id) AS num_employees
FROM departments d
LEFT JOIN employees e ON d.department_name = e.department
GROUP BY d.department_name;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Date and Time Functions
-- MAGIC Question:
-- MAGIC Find all orders placed in the last 7 days.

-- COMMAND ----------

SELECT *
FROM orders
WHERE order_date >= CURDATE() - INTERVAL 7 DAY;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Updating Records
-- MAGIC Question:
-- MAGIC Increase the salary of all employees in the 'Engineering' department by 10%.

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,update salary
UPDATE employees
SET salary = salary * 1.10
WHERE department = 'Engineering';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Ranking and Window Functions
-- MAGIC Question:
-- MAGIC Use DENSE_RANK() to rank employees within their department, ensuring no gaps in rank values.

-- COMMAND ----------

SELECT 
    name, 
    department, 
    salary, 
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
FROM employees;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's illustrate the differences between RANK(), DENSE_RANK(), and ROW_NUMBER() using SQL queries with example data.
-- MAGIC Step 1: Create the Example Table and Insert Data
-- MAGIC First, let's create an example table called sales and insert some data into it.

-- COMMAND ----------

CREATE TABLE sales (
    id INT,
    employee_name VARCHAR(100),
    department VARCHAR(100),
    sales_amount DECIMAL(10, 2)
);

INSERT INTO sales (id, employee_name, department, sales_amount) VALUES
(1, 'Alice', 'Electronics', 3000.00),
(2, 'Bob', 'Electronics', 2000.00),
(3, 'Charlie', 'Electronics', 3000.00),
(4, 'David', 'Furniture', 4000.00),
(5, 'Eve', 'Furniture', 5000.00),
(6, 'Frank', 'Furniture', 4000.00);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 2: Use RANK(), DENSE_RANK(), and ROW_NUMBER() in Queries
-- MAGIC Now, let's run queries using RANK(), DENSE_RANK(), and ROW_NUMBER() to see how they differ.
-- MAGIC
-- MAGIC Query with RANK()
-- MAGIC The RANK() function assigns ranks with gaps if there are ties.

-- COMMAND ----------

select s.*,rank() over(partition by department ORDER BY sales_amount desc) as rank from sales s;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query with DENSE_RANK()
-- MAGIC The DENSE_RANK() function assigns ranks without gaps, even if there are ties.

-- COMMAND ----------

SELECT 
    employee_name,
    department,
    sales_amount,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY sales_amount DESC) AS dense_rank
FROM sales;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query with ROW_NUMBER()
-- MAGIC The ROW_NUMBER() function assigns a unique row number to each row, regardless of ties.

-- COMMAND ----------

SELECT 
    employee_name,
    department,
    sales_amount,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY sales_amount DESC) AS row_number
FROM sales;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC PySpark Notes:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create an RDD
-- MAGIC rdd = spark.sparkContext.parallelize([
-- MAGIC     Row(id=1, name="Alice", age=30),
-- MAGIC     Row(id=2, name="Bob", age=34)
-- MAGIC ])
-- MAGIC
-- MAGIC # Convert RDD to DataFrame
-- MAGIC df = spark.createDataFrame(rdd)
-- MAGIC df.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("hello")

-- COMMAND ----------


