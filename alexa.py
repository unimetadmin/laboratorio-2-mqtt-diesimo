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
    client =paho.mqtt.client.Client("alexa", False)
    client.qos=0
    api_address='https://api.openweathermap.org/data/2.5/weather?q=caracas&appid=97a0b245914f8b033ef75b7fc078f580&units=metric'
    json_data= requests.get(api_address).json()
    
    client.connect(host='localhost')
    minutos=0
    while True:
        minutos +=1
        
        dateminut= datetime.datetime.now()
        dateminut=dateminut+datetime.timedelta(minutes=minutos)
        info={
            "id_inst":2,
            "temp": json_data['main']['temp'],
            "time":str(dateminut)

        }
        client.publish('casa/sala/alexa', json.dumps(info), qos=0)
        print(info)
        time.sleep(1)
       
if __name__ == '__main__':
	main()
	sys.exit(0)
