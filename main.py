

import json
import boto3
import base64
import prestodb
from botocore.exceptions import ClientError

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

