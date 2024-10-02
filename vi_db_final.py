import psycopg2
import csv
import math

# As the batch number is not in the VI db, it is obtained from serial_number
def extract_batch_number(serial_number):
    try:
        parts = serial_number.split('-')
        if len(parts) > 1 and len(parts[1]) >= 3:
            return parts[1][1:3]  # Extract 2nd and 3rd digits after '-' in the serial number
        else:
            return "N/A"
    except Exception as e:
        return "N/A"

# Database connection details
host = ""
port = "6604"
dbname = "ot_hybrids"
user = ""
password = ""

# Connect to the PostgreSQL database
try:
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    print("Connection to PostgreSQL successful")

    cursor = connection.cursor()

    # The SQL query to select specific columns and to get hybrids after 2024-02-01
    query = """
        SELECT name, date_inspected, serial_number, hybrid_status, prototype_usability, folder
        FROM reports
        WHERE date_inspected > '2024-02-01'
    """

    cursor.execute(query)

    # Fetch the results
    rows = cursor.fetchall()

    # Sort rows by the "date_inspected" in ascending order
    rows.sort(key=lambda x: x[1])

    # Define the column names similar to what we need for CMSDB"
    column_names = [
        "Person responsible for the VI",  # name
        "Date",  # date_inspected
        "Serial number",  # serial_number
        "Batch number",  # batch number we derived from the "Serial number"
        "Hybrid status",  # hybrid_status
        "Usability for prototypes",  # prototype_usability
        "Folder"  # folder
    ]

    # Number of rows per CSV file as it is not possible to insert more in CMSDB at a time
    rows_per_file = 70

    # Calculate how many files are needed
    total_rows = len(rows)
    num_files = math.ceil(total_rows / rows_per_file)

    # Write the rows to multiple CSV files
    for i in range(num_files):
        csv_file = f"VI_res_part_{i + 1}.csv"

        # Determine the start and end index for each file
        start_index = i * rows_per_file
        end_index = start_index + rows_per_file
        rows_subset = rows[start_index:end_index]

        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(column_names)  # Write new headers

            # Write each row"
            for row in rows_subset:
                batch_number = extract_batch_number(row[2])
                new_row = list(row[:3]) + [batch_number] + list(row[3:])
                writer.writerow(new_row)

        print(f"VI data successfully exported to {csv_file}")

except Exception as error:
    print(f"Error connecting to PostgreSQL: {error}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
        print("PostgreSQL connection closed")

