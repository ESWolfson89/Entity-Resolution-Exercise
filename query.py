
'''"
Create table in SQL corresponding to RapidData contacts
'''
create_rapid_data = """
                       CREATE TABLE rapid_data_contacts(
                       name VARCHAR(255),
                       email_address VARCHAR(255),
                       phone_number VARCHAR(255),
                       title VARCHAR(255),
                       company_name VARCHAR(255),
                       company_domain VARCHAR(255),
                       created_at DATE,
                       updated_at DATE,
                       ip_address VARCHAR(255),
                       intent_signals VARCHAR(255),
                       do_not_call BIT
                       ); 
                    """

'''
Create table in SQL corresponding to Acme contacts
'''
create_acme = """
                 CREATE TABLE acme_contacts (
                 name VARCHAR(255),
                 email_address VARCHAR(255),
                 phone_number VARCHAR(255),
                 title VARCHAR(255),
                 company_name VARCHAR(255),
                 company_domain VARCHAR(255),
                 created_at DATE,
                 updated_at DATE,
                 country VARCHAR(255),
                 company_industry VARCHAR(255),
                 company_employees INTEGER,
                 company_revenue INTEGER); 
              """

'''
Create table in SQL corresponding to CRM contacts
'''
create_crm = """
                CREATE TABLE crm_contacts (
                name VARCHAR(255),
                email_address VARCHAR(255),
                phone_number VARCHAR(255),
                title VARCHAR(255),
                company_name VARCHAR(255),
                company_domain VARCHAR(255),
                created_at DATE,
                updated_at DATE,
                favorite_color VARCHAR(255)); 
             """

'''
Merge determined RapidData duplicates taking the the greatest of the updated_at values and 
least of created_at values for those records. I did this based on ip_address and name, which
might have downsides as discussed in the readme and powerpoint
Create a sub query that takes common name and ip_addresses and inner join the rapid_data_contacts
table with it
'''
create_reduced_rapid_data_view = """
    CREATE VIEW rd_duplicates_removed
    AS SELECT rdc.name,
              rdc.email_address,
              rdc.phone_number,
              rdc.title,
              rdc.company_name,
              rdc.company_domain,
              rdc.ip_address,
              rdc.intent_signals,
              rdc.do_not_call,
              subset.created_at,
              subset.updated_at
       FROM rapid_data_contacts as rdc
       INNER JOIN
       (
           SELECT name,
                  ip_address,
                  MIN(created_at) as created_at,
                  MAX(updated_at) as updated_at
           FROM rapid_data_contacts
           GROUP BY name,
                    ip_address
                  
       ) AS subset
       ON subset.name = rdc.name AND
          subset.ip_address = rdc.ip_address AND
          subset.updated_at = rdc.updated_at
"""

'''
Merge the view created above with the crm_contacts table
Take non-null values from full join with COALESCE and attempt
to extrapolate empty names. Take the exactly equivalent records
based on the six common columns and merge together ip_address,
intent_signals, do_not_call, and favorite color into one record
Leave both date values for both (or 3 or more) records.
'''
create_rapid_data_crm_combined_view = """
    CREATE VIEW rdc_combined 
    AS SELECT COALESCE(r.name, c.name) as name, 
              COALESCE(r.email_address, c.email_address) as email_address, 
              COALESCE(r.phone_number, c.phone_number) as phone_number, 
              COALESCE(r.title, c.title) as title,
              COALESCE(r.company_name, c.company_name) as company_name,
              COALESCE(r.company_domain, c.company_domain) as company_domain,
              r.ip_address,
              r.intent_signals,
              r.do_not_call,
              c.favorite_color,
              r.updated_at AS ru, 
              r.created_at AS rc,
              c.updated_at AS cu, 
              c.created_at AS cc 
       FROM rd_duplicates_removed AS r 
       FULL OUTER JOIN crm_contacts AS c 
       ON (c.name = r.name OR c.name = \'N/A\' OR r.name = \'N/A\') AND
           c.email_address = r.email_address AND 
          (c.company_name = r.company_name OR
           c.company_domain = r.company_domain);
"""

'''
Merge the view created above with the acme_contacts table
Take non-null values from full join with COALESCE and attempt
to extrapolate empty names. Take the exactly equivalent records
based on the six common columns and merge together ip_address,
intent_signals, do_not_call, favorite color, country, company_name,
company_employees, company_revenue, and company_industry into one record
Leave all date values for both (or 3 or more) records.
'''
create_total_combined_view_dates = """
    CREATE VIEW total_combined_dates
    AS SELECT COALESCE(a.name, rdc.name) as name, 
              COALESCE(a.email_address, rdc.email_address) as email_address, 
              COALESCE(a.phone_number, rdc.phone_number) as phone_number, 
              COALESCE(a.title, rdc.title) as title,
              COALESCE(a.company_name, rdc.company_name) as company_name,
              COALESCE(a.company_domain, rdc.company_domain) as company_domain,
              a.company_industry,
              a.company_employees,
              a.company_revenue,
              a.country,
              rdc.ip_address,
              rdc.intent_signals,
              rdc.do_not_call,
              rdc.favorite_color,
              a.updated_at as au, 
              a.created_at as ac, 
              rdc.ru as ru, 
              rdc.rc as rc, 
              rdc.cu as cu, 
              rdc.cc as cc
       FROM acme_contacts AS a 
       FULL OUTER JOIN rdc_combined AS rdc 
       ON (a.name = rdc.name OR a.name = \'N/A\' OR rdc.name = \'N/A\') AND
           a.email_address = rdc.email_address AND 
          (a.company_name = rdc.company_name OR
           a.company_domain = rdc.company_domain);
"""

'''
Query all the columns from the total_combined table combining the 
updated_at and created_at columns for each contact source into the largest
updated_at value for that record for updated_at, and smallest created_at value
for that record. This was done to preserve as much information as possible going from
six values to just 2
'''
create_total_combined_view = """
    CREATE VIEW total_combined 
    AS SELECT name, 
              email_address, 
              phone_number, 
              country,
              favorite_color,              
              title, 
              company_name, 
              company_domain, 
              company_revenue,
              company_employees,              
              company_industry,
              intent_signals, 
              do_not_call, 
              LEAST(rc, ac, cc) as created_at,
              GREATEST(ru, au, cu) as updated_at
    FROM total_combined_dates
"""

'''
rapid_data_contacts insert query prefix for adding values of dataframe into an SQL row
'''
rapid_data_insert = "INSERT INTO rapid_data_contacts VALUES("

'''
acme_contacts insert query prefix for adding values of dataframe into an SQL row
'''
acme_insert = "INSERT INTO acme_contacts VALUES("

'''
crm_contacts insert query prefix for adding values of dataframe into an SQL row
'''
crm_insert = "INSERT INTO crm_contacts VALUES("

'''
Query to check if table exists so that it can be created if it doesn't
'''
check_table_query = "SELECT * FROM sys.tables WHERE name = "