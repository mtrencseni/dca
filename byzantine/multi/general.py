import sys, threading, requests, logging
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
from collections import Counter
from bc import ByzantineConsensus

logging.getLogger("werkzeug").setLevel(logging.ERROR)

if len(sys.argv) != 5:
    print("Usage: general.py <id> <n> <m> <faulty>"); sys.exit(1)

node_id, n, m, is_traitor = map(int, sys.argv[1:])
is_traitor = bool(is_traitor)
bcr = {} # Byzantine Consensus rounds

app      = Flask(__name__)
session  = requests.Session()
executor = ThreadPoolExecutor(max_workers=64)

def majority(values, tie_breaker=min):
    if not values:
        raise ValueError("majority() called with no values")
    counter      = Counter(values)
    top_count    = max(counter.values())
    tied_values  = [v for v, c in counter.items() if c == top_count]
    # single clear winner
    if len(tied_values) == 1:
        return tied_values[0]
    # tie -> delegate to tie-breaker
    return tie_breaker(tied_values)

def async_order(target_id, msg):
    def _post():
        try: session.post(f"http://127.0.0.1:{8000+target_id}/order", json=msg)
        except: pass
    executor.submit(_post)

def traitor_timeout(v):
    return None if is_traitor else v

def new_bcr(round_id):
    return ByzantineConsensus(
        node_id=node_id, n=n, m=m,
        majority_func=majority,
        send_func=lambda target_id, msg: async_order(target_id, {**msg, "round_id": round_id}),
        next_value_func=traitor_timeout)

@app.route("/order", methods=["POST"])
def order():
    msg = request.get_json()
    if "round_id" not in msg:
        return "specify round_id", 400
    if msg["round_id"] not in bcr:
        bcr[msg["round_id"]] = new_bcr(msg["round_id"])
    bcr[msg["round_id"]].onmessage(msg)
    return jsonify(ok=True)

@app.route("/start", methods=["POST"])
def start():
    msg = request.get_json()
    if "round_id" not in msg:
        return "specify round_id", 400
    if msg["round_id"] in bcr:
        return "round_id already seen", 400
    bcr[msg["round_id"]] = new_bcr(msg["round_id"])
    bcr[msg["round_id"]].start(msg["order"])
    return jsonify(ok=True)

@app.route("/status")
def status():
    msg = request.get_json()
    if "round_id" not in msg:
        return "specify round_id", 400
    if msg["round_id"] not in bcr:
        return "no such round_id seen", 400
    return jsonify(done=bcr[msg["round_id"]].is_done())

@app.route("/decide", methods=["POST"])
def decide():
    msg = request.get_json()
    if "round_id" not in msg:
        return "specify round_id", 400
    if msg["round_id"] not in bcr:
        return "no such round_id seen", 400
    return jsonify(value=bcr[msg["round_id"]].decide(timeout_default=msg.get("timeout_default", "")))

if __name__ == "__main__":
    app.run(port=8000 + node_id, threaded=True)
