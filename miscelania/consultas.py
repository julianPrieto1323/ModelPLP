import psycopg2
import pandas as pd
# Función para ejecutar una consulta SQL

#Funcion para ejecutar una query
def execute_query(query):
    conn = psycopg2.connect(
        host="localhost",
        database="sympuf",  # Nombre de tu base de datos
        user="postgres",
        password="Guadalix12_!",
        port= "7777"
    )
    cur = conn.cursor()
    print("Conexion existosa! ")
    cur.execute(query)
    print("Query ejecutada! ")
    df = pd.read_sql_query(query, conn)
    print("Datos cargados exitosamente! ")
    conn.commit()
    cur.close()
    conn.close()

    return df

#Función para crear una cohorte
import psycopg2
from psycopg2 import sql

def create_cohort(db_params, query):
    """
    Crea una tabla de cohorte en la base de datos y llena la tabla con datos según la consulta dada.

    :param db_params: Diccionario con parámetros de conexión a la base de datos.
    :param query: Consulta SQL para seleccionar los datos a insertar en la tabla de cohorte.
    """
    # Conectar a la base de datos
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Crear la tabla 'cohorte' si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cohorte (
        desynpuf_id VARCHAR,
        bene_birth_dt INT,
        bene_death_dt INT,
        bene_sex_ident_cd INT,
        bene_race_cd INT,
        bene_esrd_ind VARCHAR,
        sp_state_code INT,
        bene_county_cd INT,
        bene_hi_cvrage_tot_mons INT,
        bene_smi_cvrage_tot_mons INT,
        bene_hmo_cvrage_tot_mons INT,
        plan_cvrg_mos_num INT,
        sp_alzhdmta INT,
        sp_chf INT,
        sp_chrnkidn INT,
        sp_cncr INT,
        sp_copd INT,
        sp_depressn INT,
        sp_diabetes INT,
        sp_ischmcht INT,
        sp_osteoprs INT,
        sp_ra_oa INT,
        sp_strketia INT,
        medreimb_ip FLOAT,
        benres_ip FLOAT,
        pppymt_ip FLOAT,
        medreimb_op FLOAT,
        benres_op FLOAT,
        pppymt_op FLOAT,
        medreimb_car FLOAT,
        benres_car FLOAT,
        pppymt_car FLOAT,
        clm_id INT,
        clm_from_dt INT,
        clm_thru_dt INT,
        icd9_dgns_cd_1 VARCHAR,
        prf_physn_npi_1 FLOAT,
        hcpcs_cd_1 VARCHAR,
        line_nch_pmt_amt_1 FLOAT,
        line_bene_ptb_ddctbl_amt_1 FLOAT,
        line_coinsrnc_amt_1 FLOAT,
        line_prcsg_ind_cd_1 VARCHAR,
        line_icd9_dgns_cd_1 VARCHAR
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Insertar los datos en la tabla 'cohorte'
    insert_query = """
    INSERT INTO cohorte (
        desynpuf_id, bene_birth_dt, bene_death_dt, bene_sex_ident_cd, bene_race_cd,
        bene_esrd_ind, sp_state_code, bene_county_cd, bene_hi_cvrage_tot_mons, bene_smi_cvrage_tot_mons,
        bene_hmo_cvrage_tot_mons, plan_cvrg_mos_num, sp_alzhdmta, sp_chf, sp_chrnkidn, sp_cncr,
        sp_copd, sp_depressn, sp_diabetes, sp_ischmcht, sp_osteoprs, sp_ra_oa, sp_strketia,
        medreimb_ip, benres_ip, pppymt_ip, medreimb_op, benres_op, pppymt_op, medreimb_car,
        benres_car, pppymt_car, clm_id, clm_from_dt, clm_thru_dt, icd9_dgns_cd_1, prf_physn_npi_1,
        hcpcs_cd_1, line_nch_pmt_amt_1, line_bene_ptb_ddctbl_amt_1, line_coinsrnc_amt_1, line_prcsg_ind_cd_1,
        line_icd9_dgns_cd_1
    )
    SELECT
        desynpuf_id, bene_birth_dt, bene_death_dt, bene_sex_ident_cd, bene_race_cd,
        bene_esrd_ind, sp_state_code, bene_county_cd, bene_hi_cvrage_tot_mons, bene_smi_cvrage_tot_mons,
        bene_hmo_cvrage_tot_mons, plan_cvrg_mos_num, sp_alzhdmta, sp_chf, sp_chrnkidn, sp_cncr,
        sp_copd, sp_depressn, sp_diabetes, sp_ischmcht, sp_osteoprs, sp_ra_oa, sp_strketia,
        medreimb_ip, benres_ip, pppymt_ip, medreimb_op, benres_op, pppymt_op, medreimb_car,
        benres_car, pppymt_car, clm_id, clm_from_dt, clm_thru_dt, icd9_dgns_cd_1, prf_physn_npi_1,
        hcpcs_cd_1, line_nch_pmt_amt_1, line_bene_ptb_ddctbl_amt_1, line_coinsrnc_amt_1, line_prcsg_ind_cd_1,
        line_icd9_dgns_cd_1
    FROM (
        """ + query + """
    ) AS source;
    """
    cur.execute(insert_query)
    conn.commit()

    # Cerrar la conexión
    cur.close()
    conn.close()

    print("Tabla 'cohorte' creada y datos insertados exitosamente.")