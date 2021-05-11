import sys
import paho.mqtt.client
import json
import numpy as np
import requests
import psycopg2 #libreria para hace coneccion con la base de datos'''
from psycopg2 import Error

#FUNCION GENERAL PARA LOS INSERT CON SUS VALORES DEFINIDOS
def nevera_olla(aux,param):
    if(aux=='nevera'):
        retorno={'insert':"""INSERT INTO nevera(id_instru, temp,cap_hielo,time_hielo,time_temp) VALUES (%s, %s, %s, %s,%s)""",
        "to_insert":(param['id_instru'], param['tem_neve'], param['cap_hielo'],param['tim_hielo'], param['tim_neve']) }
        return retorno
    elif (aux=='olla'):
        retorno={'insert':"""INSERT INTO olla(id_instru,temp,time) VALUES (%s, %s, %s)""",
        "to_insert":(param["id_instru"], param["tem_olla"], param["time"]) }
        return retorno

#FUNCION PARA SUBIR LOS DATOS RECOPILADOS A LA BASE DE DATOS
def subida(aux,param,alert):

    
    try: 
        print("--SUBIDA--")
        #ME CONECTO A LA BASE DE DATOS
        connection = psycopg2.connect(user='cfqtfkmj',
        password='zW1GB2HJTjxsbOOkP7AIJBs8ZeviBGOh',
        host="queenie.db.elephantsql.com",
        port='5432',
        database='cfqtfkmj')
        cursor = connection.cursor()
        
        #LLAMO A LA FUNCION PARA SABER CUAL INSER IMPLEMENTAR 
        dic=nevera_olla(aux,param)
        
        insert_query=dic["insert"]
        
        to_insert=dic["to_insert"]
        print(to_insert)

        cursor.execute(insert_query, to_insert) 
        #PARA GUARDAR EL MENSAJE QUE EMITE EN LA BASE DE DATOS DEPENDIENDO DEL INSTRUMENTO QUE ESTA ENVIANDO EL MENSAJE
        if(alert!= None):
            if(aux=='nevera'):
            
                print(alert['alert1'])
                print(alert['alert2'])
                insert_query_alert="""INSERT INTO mensaje(mensaje,id_habi,time,id_instr) VALUES (%s, %s,%s,%s)"""
                to_insert1=(alert['alert1'],1,to_insert[4],1)
                to_insert2=(alert['alert2'],1,to_insert[3],1)
                cursor.execute(insert_query_alert,to_insert2)
                cursor.execute(insert_query_alert,to_insert1)
            elif(aux=='olla'):
                insert_query_alert="""INSERT INTO mensaje(mensaje,id_habi,time,id_instr) VALUES (%s, %s,%s,%s)"""
                print(alert)
                to_insert=(alert,1,str(to_insert[2]),2)
                
                cursor.execute(insert_query_alert,to_insert)
                

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
    print('connected (%s)' % client._client_id)
    client.subscribe(topic='casa/cocina/#', qos=2)
    


def on_message(client,userdata,message):
    print('-----------------------------')
    print('topic: %s' % message.topic)
    if(message.topic =='casa/cocina/nevera'):
        print('--NEVERA--')
        #EXTRAIGO LA INFORMACION QUE RECIBE PARA CONVERTIRLA EN JSON
        m_decode=str(message.payload.decode("utf-8","ignore"))
        m_in=json.loads(m_decode)
        alert=None
        
       #MENSAJES DE INFORAMCION
        alert={'alert1':("La temperatura de de la nevera es: " + str(m_in['tem_neve'])),
        'alert2':("La capacidad de hielo es: "+ str(m_in['cap_hielo']))}
        print(alert['alert1'])
        print(alert['alert2'])
        #LLAMO FUNCION SUBIDA DONDE SE SUBEN LOS DATOS A LA BASE DE DATOS
        subida('nevera',m_in,alert)

    elif(message.topic =='casa/cocina/olla'):
        print('--OLLA--')
        m_decode=str(message.payload.decode("utf-8","ignore"))
        m_in=json.loads(m_decode)
        alert=None

        #MENSAJES DE INFORAMCION SI ES MAYOR A 100
        if(m_in['tem_olla'] >100):
            alert=("ALERTA! La temperatura de la olla es: " + str(m_in['tem_olla'] ))
            print(alert)
        else:
            alert=("La temperatura es: "+ str(m_in['tem_olla']) + '°C')
            print(alert)
       
        subida('olla',m_in,alert)
    

    
    
  

def main():
	client = paho.mqtt.client.Client(client_id='casa_subs', clean_session=False)
	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(host='127.0.0.1', port=1883)
	client.loop_forever()


if __name__ == '__main__':
    main()
    sys.exit(0)