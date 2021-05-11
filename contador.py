import numpy as np
import paho.mqtt.client
import paho.mqtt.publish
import datetime 
import json
import time
import ssl
import sys



def on_connect(cliente, userdata,flags,rc):
    print('conectando al publicador')

def main():
    client =paho.mqtt.client.Client("persona", False)
    client.qos=0

    client.connect(host='localhost')
    minutos=0
    horas=0
    while True:
        
        minutos +=1
        cant=int(np.random.uniform(0,10))
        dateminut= datetime.datetime.now()
        dateminut=dateminut+datetime.timedelta(minutes=minutos)

        info={
            "id_inst":1,
            "cant": cant,
            "time":str(dateminut)

        }
        client.publish('casa/sala/persona', json.dumps(info), qos=0)
        print(info)
        time.sleep(1)
       
if __name__ == '__main__':
	main()
	sys.exit(0)
