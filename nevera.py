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
    client =paho.mqtt.client.Client("nevera", False)
    client.qos=0

    client.connect(host='localhost')
    minutos1=0
    minutos2=0
    while True:
        
        tem_neve=round(np.random.normal(10,2), 4)
        cap_hielo=round(np.random.uniform(0,10), 4)
        h_extra= datetime.datetime.now()
        minutos1+=10
        minutos2+=5
        datehora1=h_extra+datetime.timedelta(minutes=minutos1)
        datehora2=h_extra+datetime.timedelta(minutes=minutos2)
        info={
            "id_instru":1,
            "tem_neve": tem_neve,
            "cap_hielo": cap_hielo,
            "tim_hielo":str(datehora1),
            "tim_neve":str(datehora2)

        }
        client.publish('casa/cocina/nevera', json.dumps(info), qos=0)
        print(info)
        time.sleep(1)
       
if __name__ == '__main__':
	main()
	sys.exit(0)
