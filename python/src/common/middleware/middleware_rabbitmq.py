import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})

        self.host = host
        self.queue_name = queue_name
        self.channel = channel


    #Comienza a escuchar a la cola e invoca a on_message_callback tras
    #cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, _, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)

                on_message_callback(body, ack, nack)

            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
        

    #Si se estaba consumiendo desde la cola, se detiene la escucha. 
    #Si no se estaba consumiendo de la cola, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(e)

    #Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
    
    #Se desconecta de la cola al que estaba conectado.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            pass
        
        except Exception as e:
            raise MessageMiddlewareCloseError(e)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
