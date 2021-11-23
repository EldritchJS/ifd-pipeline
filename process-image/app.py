import argparse
import logging
import os
import time
import urllib.request as urllib
from json import loads
from json import dumps
import numpy as np
import requests
from io import BytesIO
from kafka import KafkaConsumer
from kafka import KafkaProducer

def main(args):
    batch_count = 0

    logging.info('brokers={}'.format(args.brokers))
    logging.info('readtopic={}'.format(args.readtopic))
    logging.info('creating kafka consumer')

    consumer = KafkaConsumer(
        args.readtopic,
        bootstrap_servers=args.brokers,
        value_deserializer=lambda val: loads(val.decode('utf-8')))
    logging.info("finished creating kafka consumer")

    producer = KafkaProducer(
        bootstrap_servers=args.brokers,
        value_serializer=lambda x: dumps(x).encode('utf-8'))
    logging.info('finished creating kafka producer')    

    while True:
        for message in consumer:
            if message.value['url']:
                image_url = message.value['url']
                response = requests.get(image_url)
                img = Image.open(BytesIO(response.content))
                infilename = message.value['filename'].rpartition('.')[0]
                logging.info('received URL {}'.format(image_url))
                logging.info('received label {}'.format(label))
                logging.info('received filename {}'.format(infilename))
                image = np.array(img.getdata()).reshape(1,img.size[0], img.size[1], 3).astype('float32')
                images = np.ndarray(shape=(2,32,32,3))
                logging.info('created images storage')
                images[0] = image
                logging.info('assigned image to images')
                fs=BytesIO()
#                imout=Image.fromarray(np.uint8(adversarial[0]))
#                imout.save(fs, format='jpeg')
#                outfilename = '/images/{}_{}_adv.jpg'.format(infilename,adv_inf) 
#                logging.info('Uploading file')

def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, "") != "" else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.readtopic = get_arg('KAFKA_READ_TOPIC', args.readtopic)
    args.writetopic = get_arg('KAFKA_WRITE_TOPIC', args.writetopic)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Consume messages on Kafka, download images, generate heatmaps')
    parser.add_argument(
            '--brokers',
            help='The bootstrap servers, env variable KAFKA_BROKERS',
            default='kafka:9092')
    parser.add_argument(
            '--readtopic',
            help='Topic to read from, env variable KAFKA_READ_TOPIC',
            default='benign-images')
    parser.add_argument(
            '--writetopic',
            help='Topic to write to, env variable KAFKA_WRITE_TOPIC',
            default='benign-batch-status')    
    cmdline_args = parse_args(parser)
    main(cmdline_args)
    logging.info('exiting')

