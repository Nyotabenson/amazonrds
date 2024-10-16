import pymysql
import streamlit as st
import pandas as pd

# RDS connection parameters
db_host = 'dginventory.c30kqi2wqc61.us-east-1.rds.amazonaws.com'
db_user = 'bensonmgr'
db_password = 'bensonmgr'
db_name = 'packagingmtrs'

# Connect to the database
def connect_to_db():
    try:
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        return connection
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None
    
    # Function to fetch data from the "inbound" table
def fetch_inbound_data():
    connection = connect_to_db()
    if connection is not None:
        try:
            with connection.cursor() as cursor:
                # SQL query to select all data from the "inbound" table
                query = "SELECT * FROM inbound;"
                cursor.execute(query)
                
                # Fetch all the rows from the query result
                result = cursor.fetchall()

                column_names = [desc[0] for desc in cursor.description]

                # Combine column names with the data
                data_with_columns = [dict(zip(column_names, row)) for row in result]
                                
                # Close the connection
                connection.close()

                # Return the fetched data
                return data_with_columns
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
    else:
        print("no connection")

# Display data in the Streamlit app
def display_inbound_data():
    data = fetch_inbound_data()
    if data:
        df_inbound = pd.DataFrame(data)
        # Assuming the 'inbound' table has these columns: id, item_name, quantity, date_received
        st.write("Inbound Data:")
        st.table(df_inbound.tail(3))
    else:
        st.write("No data available or unable to fetch data.")


# Display the inbound data in the app
display_inbound_data()


########################################outbound fetch#############################

  # Function to fetch data from the "inbound" table
def fetch_outbound_data():
    connection = connect_to_db()
    if connection is not None:
        try:
            with connection.cursor() as cursor:
                # SQL query to select all data from the "inbound" table
                query = "SELECT * FROM outbound;"
                cursor.execute(query)
                
                # Fetch all the rows from the query result
                result = cursor.fetchall()

                column_names = [desc[0] for desc in cursor.description]

                # Combine column names with the data
                data_with_columns = [dict(zip(column_names, row)) for row in result]
                                
                # Close the connection
                connection.close()

                # Return the fetched data
                return data_with_columns
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
    else:
        print("no connection")

# Display data in the Streamlit app
def display_outbound_data():
    data = fetch_outbound_data()
    if data:
        df_outbound = pd.DataFrame(data)
        # Assuming the 'inbound' table has these columns: id, item_name, quantity, date_received
        st.write("outbound Data:")
        st.table(df_outbound.tail(3))
    else:
        st.write("No data available or unable to fetch data.")



# Display the inbound data in the app
display_outbound_data()


#############################Displaying the baance############################
 # Function to fetch data from the "inbound" table
def fetch_balance_data():
    connection = connect_to_db()
    if connection is not None:
        try:
            with connection.cursor() as cursor:
                # SQL query to select all data from the "inbound" table
                query = """
                               SELECT
                                    sum(o.g_printers) - sum(i.g_printers)  as b_g_printers,
                                    sum(o.clear_tapes) - sum(i.clear_tapes)  as b_clear_tapes,
                                    sum(o.branded_tapes) - sum(i.branded_tapes)  as b_branded_tapes,
                                    sum(o.plastic_bags_small) - sum(i.plastic_bags_small)  as b_plastic_bags_small,
                                    sum(o.carton_boxes_small) - sum(i.carton_boxes_small)  as b_carton_boxes_small,
                                    sum(o.carton_boxes_medium) - sum(i.carton_boxes_medium)  as b_carton_boxes_medium,
                                    sum(o.carton_boxes_large) - sum(i.carton_boxes_large)  as b_carton_boxes_large,
                                    sum(o.plastic_bags_medium) - sum(i.plastic_bags_medium)  as b_plastic_bags_medium,
                                    sum(o.kg_90_suck) - sum(i.kg_90_suck)  as b_kg_90_suck,
                                    sum(o.kg_50_suck) - sum(i.kg_50_suck)  as b_kg_50_suck
                                    FROM 
                                        outbound o,
                                        inbound i;
                                                     """
                cursor.execute(query)
                
                # Fetch all the rows from the query result
                result = cursor.fetchall()

                column_names = [desc[0] for desc in cursor.description]

                # Combine column names with the data
                data_with_columns = [dict(zip(column_names, row)) for row in result]
                                
                # Close the connection
                connection.close()

                # Return the fetched data
                return data_with_columns
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
    else:
        print("no connection")

# Display data in the Streamlit app
def display_balance_data():
    data = fetch_balance_data()
    if data:
        df_balance = pd.DataFrame(data)
        df_balance.to_csv("df_balance.csv", index=False)
    
    else:
        st.write("No data available or unable to fetch data.")
