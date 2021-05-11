import sys
import paho.mqtt.client
import json
import numpy as np
import requests
import psycopg2 #libreria para hace coneccion con la base de datos'''
from psycopg2 import Error
import datetime

def tanque(aux,param):
    if(aux=='tanque'):
        retorno={'insert':"""INSERT INTO tanque(id_instr,cant_agua ,time) VALUES (%s, %s,  %s)""",
        "to_insert":( param['id_inst'], param['cant_agua'], param['time'],) }
        return retorno
    




def subida(aux,param,alert):
    
    try: 
        print("ENTRO A SUBIDA")
        connection = psycopg2.connect(user='cfqtfkmj',
        password='zW1GB2HJTjxsbOOkP7AIJBs8ZeviBGOh',
        host="queenie.db.elephantsql.com",
        port='5432',
        database='cfqtfkmj')
        cursor = connection.cursor()
        
        dic=tanque(aux,param)
        print("SE CONECTO")
        insert_query=dic["insert"]
        print(insert_query)
        to_insert=dic["to_insert"]
        print(to_insert)
        cursor.execute(insert_query, to_insert)
        if(alert!= None):
            time= datetime.datetime.now()
            time=str(time)
            print(time)
            print(alert)
            insert_query_alert="""INSERT INTO mensaje(mensaje,id_habi,time) VALUES (%s, %s,%s)"""
            
            to_insert2=(alert,2,time)
            cursor.execute(insert_query_alert,to_insert2)

        connection.commit()
        count=cursor.rowcount
        cursor.execute("SELECT * from tanque")
        print(count, "Record inserted successfully into mobile table")
    except(Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")



def on_connect(client,userdata,flags,rc):
    
    client.subscribe(topic='casa/bath/#', qos=2)
    


def on_message(client,userdata,message):
    print('-----------------------------')

    if(message.topic =='casa/bath/tanque'):
        print('AJAA tanque')
        m_decode=str(message.payload.decode("utf-8","ignore"))
       
        print("data Received",m_decode)
        m_in=json.loads(m_decode)
        alert=None
        if(m_in['cant_agua']<50):
            alert=("ALERTA! HAY MENOS DE LA MITAD DEL TANQUE: " + str(m_in['cant_agua']) + '%')
            print(alert)
        
        subida('tanque',m_in,alert)
    
    
    #param={'value1':m_in['id_instru'],'value2':m_in['ten_neve'],'value3':m_in['time'],'value4':m_in['cap_hielo']}
    
    
  

def main():
	client = paho.mqtt.client.Client(client_id='casa_subs', clean_session=False)
	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(host='127.0.0.1', port=1883)
	client.loop_forever()


if __name__ == '__main__':
    main()
    sys.exit(0)