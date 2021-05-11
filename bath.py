import numpy as np
import paho.mqtt.client
import paho.mqtt.publish
import datetime 
import json
import time
import ssl
import sys
import requests




def on_connect(cliente, userdata,flags,rc):
    print('conectando al publicador')

def main():
    client =paho.mqtt.client.Client("tanque", False)
    client.qos=0
   
    
    client.connect(host='localhost')
    minutos=0
    minutos_inter=0
    cant_total=100
    while True:
        
        cant_baja=round(np.random.normal(0.1,0.05), 4)
       
        minutos +=10
        cant_total=cant_total-cant_baja
        minutos_inter+=10
        if(minutos_inter==30):
            cant_sube=round(np.random.normal(0.2,0.05), 4)
            cant_total=cant_total+cant_sube
            minutos_inter=0

        cant_total=round(cant_total,4)


        dateminut= datetime.datetime.now()
        dateminut=dateminut+datetime.timedelta(minutes=minutos)
        info={
            "id_inst":1,
            "cant_agua":cant_total ,
            "time":str(dateminut)

        }
        client.publish('casa/bath/tanque', json.dumps(info), qos=0)
        print(info)
        time.sleep(1)
       
if __name__ == '__main__':
	main()
	sys.exit(0)
