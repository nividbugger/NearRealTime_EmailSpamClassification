from actors import sparkConsumer, pyProducer 
import json

def getTopic(path):
    file= path+ "train.csv" #+"test.csv"
    f= open(file, "r", encoding= 'latin1')
    contents= f.read()
    f.close()
    return json.loads(contents)

def main(pah, topic 1, topic 2 ):
    topic_list= getTopic(path)
    kafka_producer= pyProducer.connectProducer()
    parsed= sparkConsumer.consumeStream(topic1)
    sparkConsumer.processStream(parsed, topic 2, topic_list, kafka_producer)
    sparkConsumer.startStreaming(sparkConsumer.ssc)
    if kafka_producer is not None:
        kafka_producer.close()

if __name__== '__main__':
    path= "C:\Users\ nived\OneDrive\Desktop\Academics\semester 6\DATABASE TECHNOLOGIES\project\spam"
    topic 1 = 'root'
    topic 2=  'spam'

main(path , topic1, topic2)