import logging
from flask import Flask, jsonify, request, abort

logging.getLogger("werkzeug").setLevel(logging.ERROR)

app = Flask(__name__)
counter = 0

@app.route("/get", methods=["GET"])
def get():
    return jsonify(value=counter)

@app.route("/set", methods=["POST"])
def set_value():
    global counter
    if not (data := request.get_json()) or "value" not in data:
        abort(400)
    counter = int(data["value"])
    return jsonify(ok=True)

if __name__ == "__main__":
    # one thread so GET and SET can interleave
    app.run(port=7000, threaded=False)
