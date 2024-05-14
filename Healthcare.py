# GROUP 3 Deliverable-4
# To create an application / command line program to perform Create, Read, Update, Delete (CRUD) Operations on our database

import mysql.connector
# Connecting from the server
conn = mysql.connector.connect(user = 'root',
                               host = 'localhost',
                               port = 3306,
                              password = '9844051923@Akshu001')
#print(conn)

cursor = conn.cursor()

cursor.execute("use healthcare22")
print("Healthcare database selected")

# To get column names of a table
def get_column_names(table_name):
    cursor.execute(f'SHOW COLUMNS FROM {table_name}')
    schema = cursor.fetchall()

    if not schema:
        return None
    else:
        column_names = [column[0] for column in schema]
        return column_names


# UPDATE operation
def update_record(table_name, column_name, new_value, recordid, update_data):
    try:
        query = f'UPDATE {table_name} SET {column_name} = "{new_value}" WHERE {table_name}ID = "{recordid}"'
        cursor.execute(query)
        if cursor.rowcount > 0:
            print(f'Record with ID {recordid} is successfully updated in the {table_name} table.')
        else:
            print(f'Record with ID {recordid} not found in the {table_name} table.')

    except Exception as e:
        print(e)

    finally:
        conn.commit()


# CREATE operation
def create_record(table_name, data):
    try:
        placeholders = ", ".join(["%s"] * len(data))
        query = f'INSERT INTO {table_name} VALUES ({placeholders})'
        cursor.execute(query, data)
        print(f'Record created successfully in the {table_name} table.')
    except Exception as e:
        conn.rollback()
        print(e)
    finally:
        conn.commit()


# READ operation
def read_records(table_name):
    try:
        cursor.execute(f'SELECT * FROM {table_name}')
        rows = cursor.fetchall()

        if not rows:
            print(f"No records found in the {table_name} table.")
        else:
            column_names = get_column_names(table_name)
            print("\n", "-" * 50)
            print(f"{' | '.join(column_names)}")
            print("-" * 50)
            for row in rows:
                print(" | ".join(map(str, row)))
            print("-" * 50)

    except Exception as e:
        print(e)
#delete operation
def delete_record(table_name, record_id):
    try:
        query = f'DELETE FROM {table_name} WHERE {table_name}ID = %s'
        cursor.execute(query, (record_id,))
        if cursor.rowcount > 0:
            print(f'Record with ID {record_id} deleted from the {table_name} table.')
        else:
            print(f'Record with ID {record_id} not found in the {table_name} table.')
    except Exception as e:
        print(e)
    finally:
        conn.commit()

# Function to get the minimum value of a column
def get_min_value(table_name, column_name):
    try:
        query = f"SELECT MIN({column_name}) FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            print(f"The minimum value in {column_name} of {table_name} is: {result[0]}")
        else:
            print("No data found.")
    except Exception as e:
        print(e)

# Function to get the maximum value of a column
def get_max_value(table_name, column_name):
    try:
        query = f"SELECT MAX({column_name}) FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            print(f"The maximum value in {column_name} of {table_name} is: {result[0]}")
        else:
            print("No data found.")
    except Exception as e:
        print(e)

# Function to calculate the average value of a column
def get_average_value(table_name, column_name):
    try:
        query = f"SELECT AVG({column_name}) FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            print(f"The average value in {column_name} of {table_name} is: {result[0]}")
        else:
            print("No data found.")
    except Exception as e:
        print(e)

# Function to get unique values of a column
def get_unique_values(table_name, column_name):
    try:
        query = f"SELECT DISTINCT {column_name} FROM {table_name}"
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            unique_values = [result[0] for result in results]
            print(f"Unique values in {column_name} of {table_name}: {unique_values}")
        else:
            print(f"No unique values found in {column_name} of {table_name}.")
    except Exception as e:
        print(f"Error fetching unique values: {e}")
#function to create record
def count_records(table_name, column_name, value_to_count):
    try:
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = %s"
        cursor.execute(query, (value_to_count,))
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        print(f"Error counting records: {e}")
        return 0
   
 # UNION operation
def Union_record(table_name1, table_name2):
       
    try:    
        cursor.execute(f'''
        SELECT * FROM {table_name1}
        UNION
        SELECT * FROM {table_name2};
        ''')
    except:
        print("Choose the tables with same number of columns")
    # Fetch and print the results
    result = cursor.fetchall()
    for row in result:
        print(row)



#Intersect

def intersect_record(table_name1, table_name2,column_name,intersect_table):
    try:
        # Simulating INTERSECT using INNER JOIN on subqueries
        
        query = f"""
        SELECT DISTINCT {intersect_table}.*
        FROM ({table_name1})
        INNER JOIN ({table_name2})
        ON {table_name1}.{column_name} = {table_name2}.{column_name}
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print("Intersection of two tables:")
            for result in results:
                print(result)
        else:
            print("No intersecting results found.")
    except Exception as e:
        print(f"Error during intersect operation: {e}")

#ntile
def ntile_table(table_name, column_name, num_buckets):
    try:
        # Validate number of buckets
        if num_buckets <= 0:
            raise ValueError("Number of buckets must be a positive integer.")
        
        # Construct the NTILE query
        query = f"""
        SELECT {column_name}, NTILE({num_buckets}) OVER (ORDER BY {column_name}) AS bucket
        FROM {table_name}
        ORDER BY {column_name}
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print(f"Data in {table_name} divided into {num_buckets} buckets based on the '{column_name}' column:")
            for result in results:
                print(f"Value: {result[0]}, Bucket: {result[1]}")
        else:
            print(f"No data found in the {table_name} table or column '{column_name}'.")
    except Exception as e:
        print(f"Error during NTILE operation: {e}")

#cummulative distribution

def cumulative_distribution(table_name, column_name):
    try:
        # Calculate the total count for normalization
        total_count_query = f"SELECT COUNT({column_name}) FROM {table_name}"
        cursor.execute(total_count_query)
        total_count = cursor.fetchone()[0]
        if total_count == 0:
            print(f"No data available in {table_name} to compute distribution.")
            return

        # Calculate cumulative distribution
        query = f"""
        SELECT {column_name}, 
               SUM({column_name}) OVER (ORDER BY {column_name}) / SUM({column_name}) OVER () AS cumulative_distribution
        FROM {table_name}
        ORDER BY {column_name}
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print(f"Cumulative distribution for {column_name} in {table_name}:")
            for result in results:
                print(f"Value: {result[0]}, Cumulative Distribution: {result[1]:.4f}")
        else:
            print(f"No results found for the cumulative distribution in {table_name}.")
    except Exception as e:
        print(f"Error during cumulative distribution calculation: {e}")


#roll up

def rollup_data(table_name, group_by_columns):
    try:
        # Converting the list of columns into a comma-separated string for SQL query
        group_by_str = ", ".join(group_by_columns)
        
        # Construct the ROLLUP query
        query = f"""
        SELECT {group_by_str}, COUNT(*), SUM(Amount)  # Replace 'some_column' with the column you want to sum
        FROM {table_name}
        GROUP BY {group_by_str} WITH ROLLUP
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print(f"ROLLUP results for {table_name} on {group_by_str}:")
            for result in results:
                print(result)
        else:
            print("No results found for the ROLLUP operation.")
    except Exception as e:
        print(f"Error during ROLLUP operation: {e}")

#CUBE Function
def cube_simulation(table_name, dimension1, dimension2):
    try:
        # Construct the simulated CUBE query using UNION ALL
        query = f"""
        SELECT {dimension1}, {dimension2}, COUNT(*) AS count, SUM(amount) AS total_amount
        FROM {table_name}
        GROUP BY {dimension1}, {dimension2}
        UNION ALL
        SELECT {dimension1}, NULL, COUNT(*), SUM(amount)
        FROM {table_name}
        GROUP BY {dimension1}
        UNION ALL
        SELECT NULL, {dimension2}, COUNT(*), SUM(amount)
        FROM {table_name}
        GROUP BY {dimension2}
        UNION ALL
        SELECT NULL, NULL, COUNT(*), SUM(amount)
        FROM {table_name}
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            print(f"CUBE results for {table_name} based on {dimension1} and {dimension2}:")
            for result in results:
                print(result)
        else:
            print("No results found for the CUBE operation.")
    except Exception as e:
        print(f"Error during CUBE simulation: {e}")








# MENU to display the tables present in the database
if __name__ == "__main__":
    while True:
        print("\nSelect a table to perform the operations:")
        print("1. Patients")
        print("2. Doctors")
        print("3. Insurance")
        print("4. Invoice")
        print("5. Payment")
        print("6. Pharmacy")
        print("7. Treatment")
        print("8. Exit")

        choice = input("Enter the number of the table: ")

        if choice == "8":
            print("Exited from Healthcare Database !!")
            break

        if choice in ["1", "2", "3", "4", "5", "6", "7"]:
            table_name = [
                "Patients",
                "Doctors",
                "Insurance",
                "Invoice",
                "Payment",
                "Pharmacy",
                "Treatment",
            ][int(choice) - 1]

            print(f"Selected table: {table_name}")

            while True:
                print("\nOptions:")
                print("1. Create a record for the selected table")
                print("2. Read all records for selected table")
                print("3. Update a record for selected table")
                print("4. Delete a record for selected table")
                print("5. Average value of a column")
                print("6. Count appearances in a column")
                print("7. Minimun value of a column")
                print("8. Maximum value of a column")
                print("9. Union Operation")
                print("10. Intersect Operation")
                print("11. NTile")
                print("12. Cumulative Distribution")
                print("13. Cube")
                print("14. Rollup") 
                print("15. Return to Main Menu")

                operation_choice = input("Enter your choice: ")

                if operation_choice == "1":
                    while True:
                        data = []
                        column_names = get_column_names(table_name)
                        print(table_name)

                        if column_names:
                            print(f"Enter data in this order:\n {', '.join(column_names)}")
                            values = input("Enter values (comma-separated):\n ").split(',')

                            if len(values) != len(column_names):
                                print("Invalid Entry!!! \n Number of values entered does not match the number of columns \n Enter Again")
                                continue
                            else:
                                data = values
                                break
                        else:
                            print(f"Table {table_name} does not exist.")
                            break
                    create_record(table_name, data)
                    break

                elif operation_choice == "2":
                    read_records(table_name)
                    break

                elif operation_choice == "3":
                    while True:
                        recordid = input(f"Enter the ID of the record you want to update in {table_name}: ")
                        cursor.execute(f'SELECT * FROM {table_name} WHERE {table_name}ID = "{recordid}"')

                        if cursor.fetchone():
                            break
                        else:
                            print(f"Record with ID {recordid} does not exist in the {table_name} table. Please enter a valid ID from {table_name}.")

                    while True:
                        print(table_name, "Table =", get_column_names(table_name))
                        column_name = input("Enter the name of the column you want to update from the above list: ")

                        if column_name not in get_column_names(table_name):
                            print(f"Column '{column_name}' does not exist in the {table_name} table\nPlease enter a valid column name from the following\n")
                            print(get_column_names(table_name))
                        else:
                            new_value = input(f"Enter the new value for the {column_name} column: ")
                            print(new_value, column_name, recordid, table_name)
                            update_data = {column_name: new_value}

                            update_record(table_name, column_name, new_value, recordid, update_data)
                            break
                    break

                elif operation_choice == "4":
                    # Add the code for the Delete operation here
                    record_id = input(f"Enter the ID of the record you want to delete from {table_name}: ")
                    delete_record(table_name, record_id)
                    break
                
                elif operation_choice == "5":
                    while True:
                        column_names = get_column_names(table_name)
                        print(column_names)
                        column_name = input("Enter the name of the column to compute the average value: ")
                        if column_name not in get_column_names(table_name):
                            print(f"Column '{column_name}' does not exist in the {table_name} table\nPlease enter a valid column name from the following\n")
                            print(get_column_names(table_name))
                        else:
                            get_average_value(table_name, column_name)
                            break
                    break

                elif operation_choice == "6":
                            while True:
                                column_names = get_column_names(table_name)
                                print(column_names)
                                column_name = input("Enter the name of the column to count records: ")
                                if column_name not in get_column_names(table_name):
                                    print(f"Column '{column_name}' does not exist in the {table_name} table\nPlease enter a valid column name from the following\n")
                                    print(get_column_names(table_name))
                                else:
                                     while True:
                                            value_to_count = input(f"Enter the value to count in the {column_name} column: ")
                                            count = count_records(table_name, column_name, value_to_count)
                                            if count == 0:
                                                print(f"No records found with the value '{value_to_count}' in the {column_name} column. Please enter a valid value.")
                                                unique_values = get_unique_values(table_name, column_name)
                                                if unique_values:
                                                    print(f"Unique values in {table_name} {column_name}: {', '.join(unique_values)}")
                                                else:
                                                    print(f"No unique values found in {column_name} of {table_name}.")
                                            else:
                                                print(f"Count of records with the value '{value_to_count}' in {column_name}: {count}")
                                            break
                                     break
                            break
                
                elif operation_choice == "7":
                    while True:
                        column_names = get_column_names(table_name)
                        print(column_names)
                        column_name = input("Enter the name of the column to compute the minimum value: ")
                        if column_name not in get_column_names(table_name):
                            print(f"Column '{column_name}' does not exist in the {table_name} table\nPlease enter a valid column name from the following\n")
                            print(get_column_names(table_name))
                        else:
                            get_min_value(table_name, column_name)
                            break
                    break
                elif operation_choice == "8":
                    while True:
                        column_names = get_column_names(table_name)
                        print(column_names)
                        column_name = input("Enter the name of the column to compute the maximum value: ")
                        if column_name not in get_column_names(table_name):
                            print(f"Column '{column_name}' does not exist in the {table_name} table\nPlease enter a valid column name from the following\n")
                            print(get_column_names(table_name))
                        else:
                            get_max_value(table_name, column_name)
                            break
                    break
                
                elif operation_choice == "9":
                    print("1. Patients")
                    print("2. Doctors")
                    print("3. Insurance")
                    print("4. Invoice")
                    print("5. Payment")
                    print("6. Pharmacy")
                    print("7. Treatment")
                    choice = int(input('Enter the second table to execute Union operation: '))
                    if 1 <= choice <= 7:
                        table_name2 = [
                        "Patients",
                        "Doctors",
                        "Insurance",
                        "Invoice",
                        "Payment",
                        "Pharmacy",
                        "Treatment",
                        ][choice - 1]
                    Union_record(table_name, table_name2)
                    break
                elif operation_choice == "10":
                    print("1. Patients")
                    print("2. Doctors")
                    print("3. Insurance")
                    print("4. Invoice")
                    print("5. Payment")
                    print("6. Pharmacy")
                    print("7. Treatment")
                    choice = int(input('Enter the second table to execute Intersect operation: '))
                    if 1 <= choice <= 7:
                        table_name2 = [
                        "Patients",
                        "Doctors",
                        "Insurance",
                        "Invoice",
                        "Payment",
                        "Pharmacy",
                        "Treatment",
                        ][choice - 1]
                        #print(get_column_names(table_name))
                    #column_name = input("Enter the column name to Intersect: ")
                    a=get_column_names(table_name)
                    b=get_column_names(table_name2)
                    if(len(a)<len(b)):
                        intersect_table=table_name
                    else:
                        intersect_table=table_name2
                    column_name = [x for x in a if x in b]

                    if column_name:
                        print("Common column is:", column_name[0])
                        intersect_record(table_name, table_name2,column_name[0],intersect_table)
                    else:
                        print("No common column to perform intersect between two records")
                    break
                elif operation_choice == "11":
                   
                    print("\nNTILE Operation Calculation")
                    print(get_column_names(table_name))
                    column_name = input("Enter the column name to partition: ")
                    try:
                        num_buckets = int(input("Enter the number of buckets to divide into: "))
                        if num_buckets > 0:
                            ntile_table(table_name, column_name, num_buckets)
                        else:
                            print("Number of buckets must be greater than zero.")
                    except ValueError:
                        print("Please enter a valid integer for the number of buckets.")
                    break

                elif operation_choice == "12":
                    print("\nCumulative Distribution Calculation")
                    print(get_column_names(table_name))
                    column_name = input("Enter the column name for which to calculate the cumulative distribution: ")
                    cumulative_distribution(table_name, column_name)

                    break
                    

                elif operation_choice == "13":
                    print("\nPerforming CUBE Operation Simulation")
                    print(get_column_names(table_name))
                    dimension1 = input("Enter the first dimension for CUBE: ")
                    dimension2 = input("Enter the second dimension for CUBE: ")
                    cube_simulation(table_name, dimension1, dimension2)
                    
                    break

                
                elif operation_choice == "14":
                    print("\nPerforming ROLLUP Operation")
                    print(get_column_names(table_name))
                    group_by_columns = input("Enter the column names to group by (comma-separated): ").split(',')
                    rollup_data(table_name, group_by_columns)
                    break

                elif operation_choice == "15":
                    break
                
                else:
                    print("Invalid Operation Choice.\nPlease enter choice from 1 to 15\n")

        else:
            print("Invalid table choice \n Enter Again from 1 to 7")

# Close communication with the MySQL database
cursor.close()
conn.close()
