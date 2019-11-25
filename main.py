#!/usr/bin/env python
# coding: utf-8

# In[215]:



import json
import boto3
import base64
import prestodb
from botocore.exceptions import ClientError

def get_secret():
    secret_name = "data/automation/presto"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        text_secret_data = get_secret_value_response['SecretString']
        host = json.loads(text_secret_data)['host']
        port = json.loads(text_secret_data)['port']
        user = json.loads(text_secret_data)['user']
        catalog = json.loads(text_secret_data)['catalog']
        return (host,port,user,catalog)   

def presto_connect_db():
    get_secrate_name = get_secret()
    conn = prestodb.dbapi.connect(
    host=get_secrate_name[0],
    port= int(get_secrate_name[1]),
    user=get_secrate_name[2],
    catalog=get_secrate_name[3],)
    cur = conn.cursor()
    
    return(cur)

def drop_old_table_MC_report():
    cur.execute("drop table agg_cyber.bf_mc_logs")
    data = cur.fetchall()
    return(data)

def create_new_table_MC_report():
    cur.execute("create table agg_cyber.bf_mc_logs as ( SELECT day, ruserid,txid,id,geo,split_part(campaign,'_',1) as campaign,campaign_name, variant_id, language,eventname, status, component,session_id, type, price,currency,server_created,source FROM cyber.logs_multi a WHERE day between date_trunc('month',now() - interval '1' month) and current_date and eventname in ('Message', 'impression','transaction') and coalesce(status,' ') in ('Shown','Closed','Action','start','charge','pick',' ') AND  (component in ('website','webhook') or component is not null) group by day, ruserid, txid, id, geo, campaign,campaign_name, variant_id, language, eventname,        status,        component,         session_id,        type,        price,        currency,        server_created,        source )")
    data = cur.fetchall()
    return(data)

def give_permission_MC_report():   
    cur.execute("grant ALL PRIVILEGES on agg_cyber.bf_mc_logs to public")
    data = cur.fetchall()
    return(data)

if __name__ == '__main__':
    get_secret()
    presto_connect_db()
    drop_old_table_MC_report()
    create_new_table_MC_report()
    give_permission_MC_report()


# In[214]:





# In[211]:





# In[212]:





# In[213]:





# In[ ]:



    


# In[ ]:





# In[201]:





# In[ ]:





# In[ ]:




