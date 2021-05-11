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
    client =paho.mqtt.client.Client("olla", False)
    client.qos=0
   
    client.connect(host='localhost')
    
    while True:
        
        tem_olla=round(np.random.uniform(0,150), 4)
        datehora= datetime.datetime.now()
        info={
            "id_instru":2,
            "tem_olla": tem_olla,
            "time":str(datehora)

        }
        client.publish('casa/cocina/olla', json.dumps(info), qos=0)
        print(info)
        time.sleep(1)
       
if __name__ == '__main__':
	main()
	sys.exit(0)
