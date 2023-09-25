# sqlite_database_operations

**Dataset Information**

New York Presbyterian (NYP) Hospital charges dataset has 6193 rows and 47 columns. The columns are 'Code (CPT/DRG)', 'Description', 'Rev Code', 'Inpatient/Outpatient', 'Gross Charges', 'Discounted Cash Price', 'Aetna', 'Cigna', 'Empire Blue Cross Blue Shield', 'Emblem Health', 'United Health Group', 'Aetna Medicare', 'AgeWell Medicare', 'Emblem Medicare', 'Empire Medicare', 'Fidelis Medicare', 'Healthfirst Medicare', 'UHC Community Plan/United Medicare', 'VNS Medicare', 'WellCare Medicare', '1199', 'Affinity Molina Essential', 'Affinity Molina Medicaid/CHP', 'Amida Care Medicaid', 'Emblem Medicaid/CHP', 'Empire Healthplus Essential', 'Empire Healthplus Exchange', 'Empire Healthplus Medicaid/CHP', 'Fidelis Essential/Exchange', 'Fidelis Medicaid/CHP', 'Healthfirst Essential/Exchange', 'Healthfirst Medicaid/CHP', 'MVP Medicaid/CHP', 'MVP Essential', 'United Community Plan Essential', 'United Community Plan Medicaid', 'VNS Medicaid', 'Consumer Health Network', 'Devon', 'Equian', 'First Health', 'Magnacare', 'Multiplan/Beechstreet/PHCS', 'QHM', 'Worldwide', 'Minimum Negotiated Charge', and 'Maximum Negotiated Charge'. After data cleaning and dropping rows with NaN 5912 rows remain. To analyze the data mean, median, mode, standard deviation, range, variance, and Interquartile range of the values for columns 'Gross Charges', 'Discounted Cash Price', 'Minimum Negotiated Charge', and 'Maximum Negotiated Charge' were evaluated. A histogram of the distribution was also generated and counts for columns such as 'Inpatient/Outpatient' and Description were examined.

Maimonides Medical Center's (MMC) dataset provides standard hospital charges as of December 2021. The dataset has 107972 rows and 3 columns. The columns are 'CDM', 'CDM_Description', 'ChargeAmount'. Data cleaning was not performed because there were no missing data or duplicated rows. To analyze the data mean, median, mode, standard deviation, range, variance, and Interquartile range of the values for column ChargeAmount were evaluated. A histogram of the distribution was also generated. Counts for columns CDM_Description and CDM were also examined.

For the New York Presbyterian (NYP) Hospital dataset, the minimum negotiated charge, maximum negotiated charge, gross charges, and discounted cash price with the highest frequency is between 0 to 500. For Maimonides Medical Center datasets, the charge amount with the highest frequency is also between 0 to 500.

**Instructions and python codes to replicate SQLite database setup.**

**For creating a table manually.**

**Import required libraries.**

import pandas as pd
from sqlalchemy import create_engine
import sqlite3


**Create and connect to the database health.db.**

conn = sqlite3.connect('health.db')
c = conn.cursor()

**Create a table and columns for the table.**

c.execute("""
            CREATE TABLE treatment_costs
                (
                    hospital_name text,
                    insurance_type text,
                    code text,
                    inpatient_or_outpatient text,
                    department text,
                    treatment_description text,
                    payment_method text,
                    total_cost_negotiated real,
                    total_cost_minimum real,
                    total_cost_maximum real
                );
          """)


conn.commit()


**Confirm that the table was created in the database.**

c.execute('''
  SELECT name
  FROM sqlite_master
  WHERE type='table';
  ''')


c.fetchall()


for value in c.fetchall():
    print(value)


c.execute('''
  SELECT * FROM treatment_costs;
''')


print(c.fetchall())


**Once confirmed, insert data into the table.**

sql_query = """


INSERT INTO treatment_costs (
  'hospital_name',
  'insurance_type',
  'code',
  'inpatient_or_outpatient',
  'department',
  'treatment_description',
  'payment_method',
  'total_cost_negotiated',
  'total_cost_minimum',
  'total_cost_maximum'
  )
  values (
    'southampton hospital',
    'aetna',
    '100',
    'outpatient',
    'walkin clinic',
    'cough',
    'cash',
    100.00,
    1.00,
    5000.00
  );


"""


print(sql_query)


c.execute(sql_query)
conn.commit()


**Check that the data was inserted.**

sql_query_2 = """


select *
from treatment_costs;


"""


c.execute(sql_query_2)
print(c.fetchall())

**Add additional data rows to the table.**

c.execute('''
    INSERT INTO treatment_costs
    VALUES
        ('NYP hospital',
        'other',
        '10',
        'outpatient',
        'Dermatology',
        'rash',
        'credit card',
        500.00,
        1.00,
        5000.00),
        ('MMC hospital',
        'other',
        '102',
        'outpatient',
        'Optometry',
        'eye exam',
        'credit card',
        450.00,
        1.00,
        5000.00)
      ''')

Commiting to database
conn.commit()


**Check that new data rows were added.**

sql_query_2 = """


select *
from treatment_costs;


"""


c.execute(sql_query_2)
print(c.fetchall())


**Instructions for Automatic table creation**


**Create and connect to the database.**


engine = create_engine('sqlite:///health.db')
pd.read_sql_query("select * from treatment_costs;", conn)


**Adding Maimonides Medical Center dataset for charges into database**


**Loading dataset**


df = pd.read_csv('/content/MMC_CDM.csv')
df.sample(15)


**Replacing column name ChargeAmount with Charge_Amount**

df.rename(columns={'ChargeAmount':'Charge_Amount'
            }, inplace=True)

df.columns


**Adding dataset to database and naming table as MMC.**

df.to_sql('MMC', conn, if_exists='replace')


**Check that the new dataset was added as table MMC.**


query = """
  select *
  from MMC
  where Charge_Amount = 45;
"""


response = pd.read_sql(query, conn)
response


**Closing connection to database.**


conn.close()
