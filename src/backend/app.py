from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/api/hello-world")
def hello_world():
    return jsonify({"Hello": "World"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)