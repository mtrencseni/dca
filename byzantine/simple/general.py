import sys, threading, requests, logging
from concurrent.futures import ThreadPoolExecutor
from collections import Counter, defaultdict
from flask import Flask, request, jsonify
from math import perm

logging.getLogger("werkzeug").setLevel(logging.ERROR)

if len(sys.argv) != 5:
    print("Usage: general.py <id> <n> <m> <faulty>")
    sys.exit(1)

app = Flask(__name__)
session = requests.Session()
executor = ThreadPoolExecutor(max_workers=64)
id, n, m, traitor = map(int, sys.argv[1:])
traitor = bool(traitor)
received_values = {}
done = False
value = None

def async_order(i, msg):
    def _post():
        try: session.post(f"http://127.0.0.1:{8000+i}/order", json=msg)
        except: pass
    executor.submit(_post)

def expected_messages(k):
    n = 3 * m + 1
    return perm(n-2, m-k)

total_expected = sum(expected_messages(k) for k in range(m+1))

# def count_expected_messages():
#     expected_messages = defaultdict(int)
#     #
#     def receive_message(node, path):
#         k = 1 + m - len(path)
#         expected_messages[node] += 1
#         if k == 0: return
#         for i in range(n):
#             if i not in path+[node]:
#                 receive_message(i, path+[node])
#     #
#     for i in range(1, n):
#         receive_message(i, path=shortest_path())
#     return expected_messages[id]

def all_messages_received():
    total_received = len(received_values)
    # print(f'node={id}: {total_received}/{total_expected}')
    if total_received < total_expected:
        return False
    return True

def majority(vals):
    c = Counter(vals)
    return "attack" if c["attack"] > c["retreat"] else "retreat"

def OM(path):
    k = 1 + m - len(path)
    value = received_values[tuple(path)]
    if k == 0:
        # print(f'node={id}: in stage {k} for path={path} I am just returning the raw received message -> {value}')
        return value
    values = [value]
    # others = []
    for i in range(n):
        if i not in list(path) + [id]:
            # others.append(i)
            values.append(OM(list(path)+[i]))
    # print(f'node={id}: in stage {k} I received from path={path} value {values[0]}, my OM(k={k-1}) values from others ({others}) are {values[1:]} -> {majority(values)}')
    return majority(values)

def broadcast(path, value):
    for i in range(n):
        if i not in path:
            async_order(i, msg={"path": path, "value": value})
 
def other_value(v):
    return "retreat" if v == "attack" else "attack"

def shortest_path():
    return list(min(received_values.keys(), key=len, default=None))

@app.route("/order", methods=["POST"])
def order():
    msg = request.get_json()
    path = list(msg["path"])
    value = msg["value"]
    k = 1 + m - len(path)
    received_values[tuple(path)] = value
    if k > 0:
        broadcast(path + [id], other_value(value) if traitor else value)
    if all_messages_received():
        global done
        done = True
    return jsonify(ok=True)

@app.route("/decide", methods=["POST"])
def decide():
    assert(done)
    global value
    if value is None:
        value = OM(path=shortest_path())
    return jsonify(value=value)

@app.route("/start", methods=["POST"])
def start():
    # /start is essentially like order with: path=[], value=order, k=0
    global value
    value = request.get_json()["order"]
    if not traitor:
        broadcast([id], value)
    else:
        for i in range(0, n):
            if i == id: continue # don't order myself
            msg = {"path": [id], "value": other_value(value) if traitor and i % 2 == 0 else value}
            async_order(i, msg)
    global done
    done=True
    return jsonify(ok=True)

@app.route("/status")
def status():
    return jsonify(done=done)

if __name__ == "__main__":
    app.run(port=8000+id, threaded=True)
