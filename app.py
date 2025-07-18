from flask import Flask, jsonify, render_template
from google.cloud import storage
import json

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def get_data():
    client = storage.Client()
    bucket = client.get_bucket("dsp-numbers-bucket")
    blob = bucket.blob("data.json")
    data = json.loads(blob.download_as_string())
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
