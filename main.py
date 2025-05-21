# STEP 1A
# Import SQL Library and Pandas
import sqlite3
import pandas as pd

# STEP 1B
# Try to connect to the database, handling potential errors
try:
    conn = sqlite3.connect("data.sqlite")
except sqlite3.Error as e:
    print(f"Error connecting to database: {e}")
    exit()

# Check if the employees table has data
try:
    employee_data = pd.read_sql("""SELECT * FROM employees""", conn)
    if employee_data.empty:
        print("Warning: Employees table is empty.")
except Exception as e:
    print(f"Error fetching employees data: {e}")
    exit()

print("\n---------------------Employee Data---------------------")
print(employee_data)
print("-------------------End Employee Data-------------------")

# STEP 2
# Select employee number and last name, handle missing data
try:
    df_first_five = pd.read_sql("""SELECT IFNULL(employeeNumber, 'Unknown') AS employeeNumber, 
                                          IFNULL(lastName, 'Unknown') AS lastName FROM employees""", conn)
    print("\n----------First Five Columns----------")
    print(df_first_five)
    print("--------------------------------------")
except Exception as e:
    print(f"Error fetching first five employees: {e}")

# STEP 3
# Reverse column order: last name before employee number
try:
    df_five_reverse = pd.read_sql("""SELECT IFNULL(lastName, 'Unknown') AS lastName, 
                                            IFNULL(employeeNumber, 'Unknown') AS employeeNumber FROM employees""", conn)
    print("\n----------Five Reverse Columns----------")
    print(df_five_reverse)
    print("----------------------------------------")
except Exception as e:
    print(f"Error fetching reversed employees: {e}")

# STEP 4
# Alias employee number as ID
try:
    df_alias = pd.read_sql("""SELECT lastName, employeeNumber AS ID FROM employees""", conn)
    print("\n----------Aliased Columns----------")
    print(df_alias)
    print("-----------------------------------")
except Exception as e:
    print(f"Error fetching aliased data: {e}")

# STEP 5
# Assign roles using CASE, handling unexpected job titles
try:
    df_executive = pd.read_sql("""
        SELECT employeeNumber, jobTitle, 
        CASE 
            WHEN jobTitle IN ('President', 'VP Sales', 'VP Marketing') THEN 'Executive'
            ELSE 'Not Executive'
        END AS role
        FROM employees
    """, conn)
    print("\n----------Executive Roles----------")
    print(df_executive)
    print("-----------------------------------")
except Exception as e:
    print(f"Error classifying executives: {e}")

# STEP 6
# Find the length of last name safely
try:
    df_name_length = pd.read_sql("""SELECT lastName, LENGTH(lastName) AS name_length FROM employees""", conn)
    print("\n----------Name Length----------")
    print(df_name_length)
    print("--------------------------------")
except Exception as e:
    print(f"Error fetching name length: {e}")

# STEP 7
# Get first two letters of job title, handling NULL values
try:
    df_short_title = pd.read_sql("""SELECT jobTitle, IFNULL(SUBSTR(jobTitle, 1, 2), 'NA') AS short_title FROM employees""", conn)
    print("\n----------Short Job Title----------")
    print(df_short_title)
    print("-----------------------------------")
except Exception as e:
    print(f"Error fetching short job titles: {e}")

# Bring in another table: orderDetails
try:
    order_details = pd.read_sql("""SELECT * FROM orderDetails""", conn)
    if order_details.empty:
        print("Warning: Order details table is empty.")
    print("\n------------------Order Details Data------------------")
    print(order_details)
    print("----------------End Order Details Data----------------")
except Exception as e:
    print(f"Error fetching order details: {e}")

# STEP 8
# Calculate total amount safely
try:
    sum_total_price = pd.read_sql("""
        SELECT ROUND(SUM(priceEach * quantityOrdered), 2) AS total_price 
        FROM orderDetails
    """, conn)
    print("\n----------Total Price----------")
    print(sum_total_price)
    print("--------------------------------")
except Exception as e:
    print(f"Error calculating total price: {e}")

# STEP 9
# Extract day, month, year from order date with validation
try:
    df_day_month_year = pd.read_sql("""
        SELECT orderDate, 
        IFNULL(STRFTIME('%d', orderDate), 'Unknown') AS day, 
        IFNULL(STRFTIME('%m', orderDate), 'Unknown') AS month, 
        IFNULL(STRFTIME('%Y', orderDate), 'Unknown') AS year
        FROM orderDetails
    """, conn)
    print("\n----------Order Date Breakdown----------")
    print(df_day_month_year)
    print("----------------------------------------")
except Exception as e:
    print(f"Error processing order dates: {e}")

# Close the connection safely
try:
    conn.close()
except Exception as e:
    print(f"Error closing the database connection: {e}")