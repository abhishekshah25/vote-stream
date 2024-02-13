import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

conf = {
  'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(conf | {
  'group.id': 'voting-group',
  'auto.offset.reset': 'earliest',
  'enable.auto.commit': False
})

producer = SerializingProducer(conf)

if __name__ == "__main__":

  conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
  cur = conn.cursor()

  candidates_query = cur.execute( """ 
  SELECT row_to_json(col) FROM (SELECT * FROM candidates) col;
  """ )

  candidates = [candidate[0] for candidate in cur.fetchall()]
  
  if len(candidates) == 0:
    raise Exception("No candidates found in the database.")
  else:
    print(candidates)

  consumer.subscribe(['voters_topic'])