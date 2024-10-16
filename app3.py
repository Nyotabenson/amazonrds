import pymysql
import streamlit as st
import pandas as pd

import app2


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

# Save inbound data to the database
def save_inbound_data(in_entry):
    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                sql = """
                INSERT INTO inbound (indate, g_printers, clear_tapes, branded_tapes, plastic_bags_small, carton_boxes_small, 
                                     carton_boxes_medium, carton_boxes_large, plastic_bags_medium, kg_90_suck, kg_50_suck)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    in_entry['Inbounding Date'], in_entry["G Printers"], in_entry['ClearTapes'], in_entry["BrandedTapes"],
                    in_entry["Plastic Bags (Small)"], in_entry["Carton Boxes (Small)"], in_entry["Carton Boxes (Medium)"],
                    in_entry["Carton Boxes (Large)"], in_entry["Plastic Bags (Medium)"], in_entry["90KGS Suck"], 
                    in_entry["50KGS Suck"]
                ))
                connection.commit()
                st.success("Inbound data saved to database.")
        except Exception as e:
            st.error(f"Error saving inbound data: {e}")
        finally:
            connection.close()

# Save outbound data to the database
def save_outbound_data(out_entry):
    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                sql = """
                INSERT INTO outbound (outdate, g_printers, clear_tapes, branded_tapes, plastic_bags_small, carton_boxes_small,
                                      carton_boxes_medium, carton_boxes_large, plastic_bags_medium, kg_90_suck, kg_50_suck, orders)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    out_entry['DATE'], out_entry["G Printers"], out_entry['ClearTapes'], out_entry["BrandedTapes"],
                    out_entry["Plastic Bags (Small)"], out_entry["Carton Boxes (Small)"], out_entry["Carton Boxes (Medium)"],
                    out_entry["Carton Boxes (Large)"], out_entry["Plastic Bags (Medium)"], out_entry["90KGS Suck"], 
                    out_entry["50KGS Suck"], out_entry["Orders"]
                ))
                connection.commit()
                st.success("Outbound data saved to database.")
        except Exception as e:
            st.error(f"Error saving outbound data: {e}")
        finally:
            connection.close()


with st.sidebar.expander('INBOUND'):
    with st.form('entry form1', clear_on_submit=True):
        in_date = st.date_input("Inbounding Date", key='in_date')
        in_GP = st.number_input("G Printers", min_value=0,  step=1, key='in_GP')
        in_CT = st.number_input("ClearTapes", min_value=0, step=1, key='in_CT')
        in_BT = st.number_input("BrandedTapes", min_value=0, step=1, key='in_BT')
        #in_A5 = st.number_input("A5 Envelopes", min_value=0, step=1, key='in_A5')
        in_PB_S = st.number_input("Plastic Bags (Small)", min_value=0, step=1, key='in_PB_S')
        in_CTN_S = st.number_input("Carton Boxes (Small)", min_value=0, step=1, key='in_CTN_S')
        in_CTN_M = st.number_input("Carton Boxes (Medium)", min_value=0, step=1, key='in_CTN_M')
        in_CTN_L = st.number_input("Carton Boxes (Large)", min_value=0, step=1, key='in_CTN_L')
        in_PB_M = st.number_input("Plastic Bags (Medium)", min_value=0, step=1, key='in_PB_M')
        in_KG90 = st.number_input("90KGS Suck", min_value=0, step=1, key='in_KG90')
        in_KG50 = st.number_input("50KGS Suck", min_value=0, step=1, key='in_KG50')            
        submitted1 = st.form_submit_button("Save outbound")
        # Example for saving inbound data
        if submitted1:
            in_entry = {
                'Inbounding Date': in_date, "G Printers": in_GP, 'ClearTapes': in_CT, "BrandedTapes": in_BT, 
                "Plastic Bags (Small)": in_PB_S, "Carton Boxes (Small)": in_CTN_S, "Carton Boxes (Medium)": in_CTN_M,
                "Carton Boxes (Large)": in_CTN_L, "Plastic Bags (Medium)": in_PB_M, "90KGS Suck": in_KG90,
                "50KGS Suck": in_KG50
            }
            save_inbound_data(in_entry)






with st.sidebar.expander('OUTBOUND'):
    with st.form('entry form2', clear_on_submit=True):        
        DT = st.date_input("DATE", key='DT')
        GP = st.number_input("G Printers", min_value=0, step=1, key='GP')
        CT = st.number_input("ClearTapes", min_value=0, step=1, key='CT')
        BT = st.number_input("BrandedTapes", min_value=0, step=1, key='BT')
        #A5 = st.number_input("A5 Envelopes", min_value=0, step=1, key='A5')
        PB_S = st.number_input("Plastic Bags (Small)", min_value=0, step=1, key='PB_S')
        CTN_S = st.number_input("Carton Boxes (Small)", min_value=0, step=1, key='CTN_S')
        CTN_M = st.number_input("Carton Boxes (Medium)", min_value=0, step=1, key='CTN_M')
        CTN_L = st.number_input("Carton Boxes (Large)", min_value=0, step=1,key='CTN_L')
        PB_M = st.number_input("Plastic Bags (Medium)", min_value=0, step=1, key='PB_M')
        PB_L = st.number_input("90KGS Suck", min_value=0, step=1, key='PB_L')
        KG50 = st.number_input("50KGS Suck", min_value=0, step=1, key='KG50')
        ODRS = st.number_input("Orders", min_value=0, step=1, key='odrs')
        submitted2 = st.form_submit_button("Save outbound")
        # Example for saving outbound data
        if submitted2:
            out_entry = {
                'DATE': DT, "G Printers": GP, 'ClearTapes': CT, "BrandedTapes": BT, "Plastic Bags (Small)": PB_S, 
                "Carton Boxes (Small)": CTN_S, "Carton Boxes (Medium)": CTN_M, "Carton Boxes (Large)": CTN_L,
                "Plastic Bags (Medium)": PB_M, "90KGS Suck": PB_L, "50KGS Suck": KG50, "Orders": ODRS
            }
            save_outbound_data(out_entry)










#-----------------------------main---------------------------------------------
 #title
st.title("DILAGAIN INVENTORY")

st.write("---")

st.write('''Dilagain Technologies appears to be a startup company focusing on providing software solutions, including web development,
          mobile applications (both native and hybrid), and business-related SaaS applications. They offer services such as Customer 
          Relationship Management (CRM), Enterprise Resource Planning (ERP), and project management solutions. Their approach revolves
            around customer satisfaction and delivering high-quality, reliable software that meets their clients' needs. They also offer
              graphic design services and specialize
          in UI/UX development for websites and mobile apps, aiming to enhance user experience and improve business processes''')



app2.display_inbound_data()

st.write("---")

app2.display_outbound_data()

st.write("---")

app2.display_balance_data()

df_b = pd.read_csv("df_balance.csv")
#st.dataframe(df_b)

col1, col2 = st.columns(2)
with col1:
    
    
    st.subheader("Available Packaging Materials:")
    st.markdown(f"(i)  :blue[**G Printers**] :  {df_b['b_g_printers'].values}")
    st.markdown(f"(i)  :blue[**ClearTapes**] :  {df_b['b_clear_tapes'].values}")
    st.markdown(f"(ii)  :blue[**BrandedTapes**] :  {df_b['b_branded_tapes'].values}")
    st.markdown(f"(v)  :red[**Cartons Small-size**] :  {df_b['b_carton_boxes_small'].values}")
    st.markdown(f"(vi)  :red[**Cartons Medium-size**] :  {df_b['b_carton_boxes_medium'].values}")
    st.markdown(f"(vii)  :red[**Cartons Large-size**] :  {df_b['b_carton_boxes_large'].values}")
    st.markdown(f"(viii) :violet[**Plastic Bags Small-size**] :  {df_b['b_plastic_bags_small'].values}")
    st.markdown(f"(ix) :green[**Plastic Bags Medium-size**] :  {df_b['b_plastic_bags_medium'].values}")
    st.markdown(f"(x) :violet[**90KGS Sucks**] :  {df_b['b_kg_90_suck'].values}")
    st.markdown(f"(xi)  :violet[**50KGS Sucks**] :  {df_b['b_kg_50_suck'].values}")