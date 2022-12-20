"""

@author: Matias Amundsen and Philippe Haavik

"""

import os
import pyodbc as odbc
import sys
import argparse
import pandas as pd
 
from azure.identity import ClientSecretCredential
from azure.mgmt.consumption import ConsumptionManagementClient

# Credentials for azure
SUBSCRIPTION_ID = 'os.environ.get("SUBSCRIPTION_ID")'
CLIENT_ID = 'os.environ.get("CLIENT_ID")'
CLIENT_SECRET = 'os.environ.get("CLIENT_SECRET")'
TENANT_ID = 'os.environ.get("TENANT_ID")'

# Credentials for database
driver = 'ODBC Driver 17 for SQL Server'
server = 'os.environ.get("db_server")'
database = 'os.environ.get("db_database")'
username = 'os.environ.get("db_user")' 
password = 'os.environ.get("db_pass")'

# String used to establish connection to database
connectionString = (('Driver={ODBC Driver 17 for SQL Server};SERVER=' + server +
                                ';DATABASE=' + database + ';UID=' + username + ';PWD={' + password + "}"))

def get_usage_data(betweenDates):
    """
    Function used to get a list of selected usage reports and add them into a dataframe

    Expects: A time window for which to collect reports from
    Modifies: Using azure consumption management client to get reports which are then put into a dataframe
    Returns: A dataframe consisting of reports from the given time window

    :param betweenDates: This is the time window used to collect reports
    """

    # Create credential object 
    credentials = ClientSecretCredential(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        tenant_id=TENANT_ID,
    )

    # Create the consumption management client
    client_consumption = ConsumptionManagementClient(credentials, SUBSCRIPTION_ID)

    # Find report location
    scope = f"/subscriptions/{SUBSCRIPTION_ID}"

    usage_from_date = betweenDates.split("/")[0]
    usage_to_date = betweenDates.split("/")[1]

    date_filter = (
        f"properties/usageStarts ge '{usage_from_date}'"
        f" AND properties/usageStart lt '{usage_to_date}'"
    )

    # Get usage reports from azure
    usage_item = client_consumption.usage_details.list(
        scope=scope,
        expand="properties/meterDetails,properties/additionalProperties",
        filter=date_filter
    )

    # Set usage_item as dict
    data = [item.as_dict() for item in usage_item]

    # Create a dataframe from dict of tuples
    df = pd.DataFrame(data)

    return df


def add_tags(df):
    """
    Function used to get data from tags and add into separate columns in orginal dataframe

    Expects: A dataframe
    Modifies: Splits tags column in dataframe into multiple columns
    Returns: A dataframe
    """

    df1 = df['tags'].apply(pd.Series)
    df1.drop(df1.columns.difference(['Environment', 'entity', 'module', 'poolName']), 1, inplace=True)

    df = pd.concat([df, df1], axis=1)
    df.rename(columns = {'Environment': 'environment'}, inplace = True)

    return df


def add_meter_details(df):
    """
    Function used to get data from meter_details and add into separate columns in orginal dataframe

    Expects: A dataframe
    Modifies: Splits meter_details column in dataframe into multiple columns
    Returns: A dataframe
    """

    df2 = df['meter_details'].apply(pd.Series)
    df2.drop(df2.columns.difference(['meter_name', 'meter_category', 'meter_sub_category', 'unit_of_measure']), 1, inplace=True)

    df = pd.concat([df, df2], axis=1)

    return df


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


def insertToDB(df, conn):
    """
    Function used to insert usage reports into database

    Expects: A dataframe and a connection to the database
    Modifies: Inserts a list made from the dataframe into the database and close the connection with the database

    :param df: A dataframe
    :param conn: Connection to the database
    """

    # Fill missing values
    df.fillna('missing', inplace=True)

    # Create list from dataframe
    list_of_tuples = list(df.itertuples(index=False))

    # Insert created list into database
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True
        sql_statement = ('''INSERT INTO r_azure (name, date, product, quantity, cost, resource_id, resource_group,
            environment, entity, module, poolName,
            meter_name, meter_category, meter_sub_category,
            unit_of_measure) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''')
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

    # Get the usage data
    df = get_usage_data(betweenDates=args.betweenDates)

    # Remove redundant columns
    df.drop(labels={'id', 'type', 'kind', 'billing_account_id',
        'billing_account_name', 'billing_period_start_date', 'billing_period_end_date',
        'billing_profile_id', 'billing_profile_name', 'account_owner_id',
        'account_name', 'subscription_id', 'subscription_name',
        'part_number', 'meter_id', 'effective_price',
        'unit_price', 'billing_currency', 'resource_location',
        'consumed_service', ' resource_name',
        'invoice_section', 'cost_center', 'reservation_id', 'reservation_name',
        'is_azure_credit_eligible', 'publisher_type', 'charge_type', 'frequency',
        'pay_g_price', 'benefit_id', 'benefit_name', 'pricing_model',
        'term', 'offer_id', 'service_info2'}, inplace=True, axis=1)

    # Get details and separate data into multiple columns from tags column
    df = add_tags(df)

    # Get details and separate data into multiple columns from meter_details column
    df = add_meter_details(df)

    # Remove tags and meter_details columns
    df.drop(labels={'tags', 'meter_details'}, inplace=True, axis=1)

    # Create connection to database
    connection = createConnection(connectionString)

    # Insert usage reports into database
    insertToDB(df, connection)


if __name__ == "__main__":
    main()
