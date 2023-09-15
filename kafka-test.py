from confluent_kafka import Consumer
import confluent_kafka


def my_assign (consumer, partitions):
    for p in partitions:
        low, high = consumer.get_watermark_offsets(p)
        p.offset = confluent_kafka.OFFSET_END
        print("offset=",p.offset)
    print('assign', partitions)
    print('position:',consumer.position(partitions))
    consumer.assign(partitions)

config_consumer = {
            'bootstrap.servers': '172.24.5.187:9094', 
            'group.id': 'group-id', #str(uuid.uuid1())[:8],
            'group.instance.id':'new-group-1',
            'auto.offset.reset': 'latest',
            'enable.auto.commit':'false',
            #'debug': 'consumer,cgrp,topic',
        }
consumer = Consumer(config_consumer)
consumer.subscribe(['app-ml2-reply'], on_assign=my_assign)
#reset_topic_offset_in_time('app-ml2-reply', time.time())


while(True):
    message = consumer.poll(0.3)
    # assigned_list = consumer.assignment()
    # if(assigned_list is not None and len(assigned_list)>0):
    #     print(assigned_list)

    if message is None:
        continue
    print(message.value()) 


