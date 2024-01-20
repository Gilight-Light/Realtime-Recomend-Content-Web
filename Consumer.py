from kafka import KafkaProducer
from json import dumps
from time import sleep
import psycopg2 


topic_name = 'recommendsystem'
kafka_server = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer = lambda x:dumps(x).encode('utf-8'))


# Connect to the database 
conn = psycopg2.connect(database="dataeng", user="postgres", 
                        password="postgres", host="localhost", port="5432") 

# create a cursor 
cur = conn.cursor() 

seed(1)

for e in range(1000):
    try:

        # Excute get data from database
        cur.execute( 
            '''SELECT * FROM ACTIONS''') 
        data_from_db = cur.fetchall()
        # commit the changes 
        conn.commit() 
        
        data = {'number' : e}
        producer.send(topic_name, value=data)
        print(str(data) + " sent")
        sleep(5)
    except KeyboardInterrupt:
            print("break")
            break
# close the cursor and connection 
cur.close() 
conn.close()   
producer.flush()