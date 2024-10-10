from gcn_kafka import Consumer
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import os

load_dotenv()

# Conecte-se como consumidor usando variáveis de ambiente
consumer = Consumer(client_id=os.getenv('KAFKA_CLIENT_ID'),
                    client_secret=os.getenv('KAFKA_CLIENT_SECRET'))

# Subscribe to topics and receive alerts
consumer.subscribe(['gcn.circulars',
                    'gcn.heartbeat',
                    'gcn.notices.icecube.lvk_nu_track_search',
                    'igwn.gwalert',
                    'gcn.notices.swift.bat.guano',
                    'gcn.notices.einstein_probe.wxt.alert'])
# while True:
#     for message in consumer.consume(timeout=1):
#         if message.error():
#             print(message.error())
#             continue
#         # Print the topic and message ID
#         print(f'topic={message.topic()}, offset={message.offset()}')
#         value = message.value()
#         print(value)

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING') # Connection string da sua Azure Storage Account
container_name = os.getenv('AZURE_CONTAINER_NAME_JSON', 'landing/kafka/json')  # Nome do container onde deseja gravar os blobs
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

while True:
    for message in consumer.consume(timeout=1):
        if message.error():
            print(message.error())
            continue

        # Exibir o tópico e o offset da mensagem
        print(f'topic={message.topic()}, offset={message.offset()}')
        value = message.value()
        print(value)

        # Gerar o nome do blob usando o tópico e o offset como identificadores únicos
        blob_name = f"{message.topic()}_{message.offset()}.json"

        # Criar o blob
        blob_client = container_client.get_blob_client(blob_name)

        # Upload do conteúdo da mensagem como blob
        blob_client.upload_blob(value)