import flask
from flask import Flask, request, jsonify
import logging

from time import sleep
from json import dumps
from kafka import KafkaProducer

app = flask.Flask(__name__)
app.config["DEBUG"] = True

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

@app.route('/', methods=['GET'])
def home():
    return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"

@app.route('/api/evaluate', methods=['POST'])
def evaluate():
    input = request.json
    logging.info('Expression to be evaluated %s', input['expression'])

    data = {'expression': input['expression']}
    producer.send('topic_test', value=data)

    return jsonify({"response":"success"})

app.run()
