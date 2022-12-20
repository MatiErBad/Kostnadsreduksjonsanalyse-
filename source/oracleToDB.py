"""

@author: Matias Amundsen and Philippe Haavik

"""

import oci
import os
import pandas as pd
import pyodbc as odbc
import sys
import argparse
from datetime import datetime
from datetime import date
import warnings

reporting_namespace = 'bling'

# Download all usage and cost files. You can comment out based on the specific need:
#prefix_file = "report/usage-csv"                         # For usage files                              
prefix_file = "report/cost-csv"                          # For cost files
#prefix_file = ""                                         # For cost and usage files

# Directory location for reports
destintation_path = 'oci_cost'

# Credentials for database
driver = '{ODBC Driver 17 for SQL Server}'
server = 'os.environ.get("db_server")'
database = 'os.environ.get("db_database")'
username = 'os.environ.get("db_user")'
password = 'os.environ.get("db_pass")'

# String used to establish connection to database
connectionString = (('Driver={ODBC Driver 17 for SQL Server};SERVER=' + server +
                                ';DATABASE=' + database + ';UID=' + username + ';PWD={' + password + "}"))

def get_object_list():
    """
    Function used to get list of all objects from the cost reports bucket

    Modifies: A cost reports bucket created with ObjectStorageClient and config (which includes credentials for oracle)
    Returns: A list of all objects from the cost reports bucket
    """

    # Reads the config file (which includes credentials for oracle) and create the object storage client
    config = oci.config.from_file(oci.config.DEFAULT_LOCATION, oci.config.DEFAULT_PROFILE)
    object_storage = oci.object_storage.ObjectStorageClient(config)

    reporting_bucket = config['tenancy']

    # Add fields name, timeCreated and size used for filtering later
    report_bucket_objects = object_storage.list_objects(reporting_namespace, reporting_bucket, fields="name,timeCreated,size", prefix=prefix_file)

    return {"object_storage": object_storage, "report_bucket_objects": report_bucket_objects, "reporting_bucket": reporting_bucket}


def filter(all_objects, destintation_path, betweenDates):
    """
    Function used to filter and download correct reports which loop through all object from the cost reports bucket

    :param all_objects: The list of all objects from the cost reports bucket
    :param destintation_path: Location where downloaded reports are placed
    :param betweenDates: This is the time window used to collect reports
    """

    # Create the destination path if it doesn't exist
    if not os.path.exists(destintation_path):
        os.mkdir(destintation_path)

    # Download only the reports that are between the two dates from betweenDates
    for o in all_objects["report_bucket_objects"].data.objects:
        if (o.time_created.date() >= datetime.strptime(betweenDates.split("/")[0], "%Y-%m-%d").date() and
                    o.time_created.date() <= datetime.strptime(betweenDates.split("/")[1], "%Y-%m-%d").date()):
                    download_report(o, destintation_path, all_objects)


def download_report(obj, destintation_path, objects):
    """
    The function use given file to download and write to disk report file.

    :param obj: A specific file from the list of all object
    :param destintation_path: Location where downloaded reports are placed
    :param objects: The list of all objects from the cost reports bucket
    """

    # Get details for the object
    object_details = objects["object_storage"].get_object(reporting_namespace, objects["reporting_bucket"], obj.name)

    # Get the last 2 parts of the filename
    filename = str(obj.time_created.date()) + "-" + obj.name.rsplit("/", 2)[-2] + "-" + obj.name.rsplit("/", 2)[-1]

    print(f"Downloading the report from {obj.time_created.date()} named {obj.name}")

    # Download and write to disk the reports
    with open(destintation_path + "/" + filename, "wb") as f:
        for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
            f.write(chunk)
    
    print(f"Finished downloading report '{obj.name}' here: '{destintation_path}/{filename}'\n")


def createConnection(connectionString):
    """
    Function used to create connection to database

    Expects: A string consisting of database credentials
    Modifies: Create a connection to the database using connectionString
    Returns: Connection to the database
    """

    try:
        conn = odbc.connect(connectionString)
        print("{c} is working".format(c=connectionString))
    except Exception as e:
        print(e)
        conn.close()
    return conn


def insertToDB(conn):
    """
    Function used to insert disk report files into database

    Expects: A connection to the database
    Modifies: Loop over each disk report file and insert each one into the database 

    :param conn: Connection to the database
    """

    # Loop over cost reports in directory
    for fname in os.listdir(destintation_path):

        # Transform data using pandas
        df = pd.read_csv(os.path.join(destintation_path, fname), compression='gzip', low_memory=False)
        with warnings.catch_warnings(record=True):
            df.drop(df.columns.difference(['lineItem/referenceNo', 'lineItem/intervalUsageStart', 'product/service', 'product/compartmentId', 'product/compartmentName', 'usage/billedQuantity', 'cost/unitPrice', 'cost/myCost', 'tags/entity', 'tags/instance', 'tags/module', 'tags/oci:compute:instanceconfiguration', 'tags/orcl-cloud.free-tier-retained']), 1, inplace=True)
        df.rename(columns = {'lineItem/referenceNo':'referenceNo', 'lineItem/intervalUsageStart':'intervalUsageStart', 'product/service':'service', 'product/compartmentId':'compartmentId', 'product/compartmentName':'compartmentName', 'usage/billedQuantity':'billedQuantity', 'cost/unitPrice':'unitPrice', 'cost/myCost':'myCost', 'tags/entity':'entity', 'tags/instance':'instance', 'tags/module':'module', 'tags/oci:compute:instanceconfiguration':'instanceconfiguration', 'tags/orcl-cloud.free-tier-retained':'free-tier-retained'}, inplace = True)

        # Fill missing values
        df['unitPrice'] = df['unitPrice'].fillna(0)
        df[['referenceNo', 'intervalUsageStart', 'service', 'compartmentId', 'compartmentName', 'entity', 'instance', 'module', 'instanceconfiguration', 'free_tier_retained']] = df[['referenceNo', 'intervalUsageStart', 'service', 'compartmentId', 'compartmentName', 'entity', 'instance', 'module', 'instanceconfiguration', 'free_tier_retained']].fillna('missing')

        # Create list from dataframe
        list_of_tuples = list(df.itertuples(index=False))

        # Insert created list into database
        try:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            sql_statement = ('''INSERT INTO r_oci (referenceNo, intervalUsageStart, service,
                compartmentId, compartmentName,
                billedQuantity, unitPrice, myCost,
                entity, instance, 
                module, instanceconfiguration,
                free_tier_retained) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''')
            cursor.executemany(sql_statement, list_of_tuples)
        except Exception as err:
            print('Error while inserting', err)
        else:
            print('Insert Completed')
            conn.commit()
            conn.close()


def main():
    """
    Main function
    """

    # Arguments to be pased when executing the script
    parser = argparse.ArgumentParser(description="Download Usage Reports")
    parser.add_argument("--betweenDates", help=f"Download all reports available between two dates (inclusive). Ex: python {sys.argv[0]} --betweenDates 2022-05-20/2022-05-23")
    args = parser.parse_args()

    # if there are no arguments passed
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit()
    # if there are more than three arguments passed
    elif len(sys.argv) > 8:
        print()
        print("ERROR - Too many arguments passed.")
        print()
        print("Please use the -h argument to find out how to use the script")
        print()
        print(f"Example: python {sys.argv[0]} -h")
        sys.exit()

    # Get the object list
    all_objects = get_object_list()

    # Find and download reports for a specific period
    filter(all_objects, destintation_path, betweenDates=args.betweenDates)

    # Create connection to database
    connection = createConnection(connectionString)

    # Insert reports into database
    insertToDB(connection)


if __name__ == "__main__":
    main()