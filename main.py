import pika
import os
import math
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

DIA_100K = 100000
DIA_5k = 5000

class Analitica():
    valor_maximo = -math.inf
    valor_minimo = math.inf
    influx_bucket = 'rabbit'
    influx_token = 'TOKEN_SECRETO'
    influx_url = 'http://influxdb:8086'
    influx_org = 'org'
    contador_paso = 0
    contador_s = 0
    dias_100k = 0
    dias_5k = 0
    valor_anterior = 0
    dias_consecutivos = 0

    def write_db(self, tag, key, value):
        client= InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        point = Point('Analitica').tag("Descripcion", tag).field(key, value)
        write_api.write(bucket=self.influx_bucket, record=point)

    def tvalor_maximo(self, _medida):
        if _medida > self.valor_maximo:
            self.valor_maximo = _medida
        self.write_db('Pasos', "Maximos", self.valor_maximo)
    
    def tvalor_minimo(self, _medida):
        if _medida < self.valor_minimo:
            self.valor_minimo = _medida
        self.write_db('Pasos', "Minimos", self.valor_minimo)

    def promedio(self, _medida):
        self.contador_paso += 1
        self.contador_s += _medida
        mediana = self.contador_s/self.contador_paso
        print("mediana {}".format(mediana), flush=True)
        self.write_db('Pasos', "mediana", mediana)

    def tdias_100k(self, _medida):
        if _medida >= 100000:
            self.dias_100k += 1
            print("dias_100k {}".format(self.dias_100k), flush=True)
        self.write_db('Pasos', "dias_100k", self.dias_100k)

    def tdias_5k(self, _medida):
        if _medida <= 50000:
            self.dias_5k += 1
            print("dias_5k {}".format(self.dias_5k), flush=True)
        self.write_db('Pasos', "dias_5k", self.dias_5k)

    def consecutivo(self, _medida):
        if _medida >= self.valor_anterior:
            self.dias_consecutivos += 1
        else:
            self.dias_consecutivos = 0
        print("dias_consecutivos {}".format(self.dias_consecutivos), flush=True)
        self.valor_anterior = _medida
        self.write_db('Pasos', "dias_consecutivos", self.dias_consecutivos)
    

    
    def toma_medida(self, _mensaje):
        mensaje = _mensaje.split("=")
        medida = float(mensaje[-1])
        print("medida {}".format(medida), flush=True)
        self.tvalor_maximo(medida)
        self.tvalor_minimo(medida)
        self.promedio(medida)
        self.tdias_100k(medida)
        self.tdias_5k(medida)
        self.consecutivo(medida)
        
if __name__ == '__main__':

  analitica = Analitica()
  def callback(ch, method, properties, body):
      global analitica
      mensaje = body.decode("utf-8")
      analitica.toma_medida(mensaje)

  url = os.environ.get('AMQP_URL', 'amqp://guest:guest@rabbit:5672/%2f')
  params = pika.URLParameters(url)
  connection = pika.BlockingConnection(params)

  channel = connection.channel()
  channel.queue_declare(queue='mensajes')
  channel.queue_bind(exchange='amq.topic', queue='mensajes', routing_key='#')    
  channel.basic_consume(queue='mensajes', on_message_callback=callback, auto_ack=True)
  channel.start_consuming()