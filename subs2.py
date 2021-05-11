import sys
import paho.mqtt.client
import json
import numpy as np
import requests
import psycopg2 #libreria para hace coneccion con la base de datos'''
from psycopg2 import Error
import datetime

def persona_alexa(aux,param):
    if(aux=='persona'):
        retorno={'insert':"""INSERT INTO persona(cant, time,id_inst) VALUES (%s, %s,  %s)""",
        "to_insert":( param['cant'], param['time'], param['id_inst'],) }
        return retorno
    elif (aux=='alexa'):
        retorno={'insert':"""INSERT INTO alexa(id_inst, temp,time) VALUES (%s, %s, %s)""",
        "to_insert":(param["id_inst"], param["temp"], param["time"]) }
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
        
        dic=persona_alexa(aux,param)
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
            insert_query_alert="""INSERT INTO mensaje(mensaje,id_habi,time,id_instr) VALUES (%s, %s,%s,%s)"""
            if(aux=='persona'):
                to_insert2=(alert,2,time,1)
                
            else:
                to_insert2=(alert,2,time,2)

            cursor.execute(insert_query_alert,to_insert2)
        connection.commit()
        count=cursor.rowcount
        cursor.execute("SELECT * from nevera")
        print(count, "Record inserted successfully into mobile table")
    except(Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")



def on_connect(client,userdata,flags,rc):
    
    client.subscribe(topic='casa/sala/#', qos=2)
    


def on_message(client,userdata,message):
    print('-----------------------------')
    
    if(message.topic =='casa/sala/persona'):
        print('AJAA persona')
        m_decode=str(message.payload.decode("utf-8","ignore"))
       
        print("data Received",m_decode)
        m_in=json.loads(m_decode)
        alert=None
        if(m_in['cant']>5):
            alert=("ALERTA! HAY MAS DE 5 PERSONAS: " + str(m_in['cant']))
            print(alert)
        
        subida('persona',m_in,alert)
    elif(message.topic =='casa/sala/alexa'):
        print('ALEXA')
        m_decode=str(message.payload.decode("utf-8","ignore"))
        
        print("data Received",m_decode)
        m_in=json.loads(m_decode)
        alert=("La temperatura es: " +str(m_in['temp'] )+ ' °C')
   
        subida('alexa',m_in,alert)
    
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