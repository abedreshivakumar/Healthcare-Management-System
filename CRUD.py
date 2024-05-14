# GROUP 3 Deliverable-4
# To create an application / command line program to perform Create, Read, Update, Delete (CRUD) Operations on our database

import mysql.connector
# Connecting from the server
conn = mysql.connector.connect(user = 'root',
                               host = 'localhost',
                               port = 3306,
                              password = 'DBO@2024')
#print(conn)

cursor = conn.cursor()

cursor.execute("use healthcare")
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
                print("5. Return to Main Menu")

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
                    break

                else:
                    print("Invalid Operation Choice.\nPlease enter choice from 1 to 4\n")

        else:
            print("Invalid table choice \n Enter Again from 1 to 7")

# Close communication with the MySQL database
cursor.close()
conn.close()
