import sys, threading, requests, logging, base64, secrets, json
from concurrent.futures import ThreadPoolExecutor
from nacl.exceptions import BadSignatureError
from collections import Counter, defaultdict
from flask import Flask, request, jsonify
from nacl import signing
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

private_key = signing.SigningKey.generate()
public_keys_cache = {}
public_keys_cache[id] = bytes(private_key.verify_key)

def get_public_key(node_id):
    global public_keys_cache
    if node_id in public_keys_cache:
        return public_keys_cache[node_id]
    public_key_b64 = requests.get(f"http://127.0.0.1:{8000+node_id}/public_key").json()["public_key"]
    public_key = base64.b64decode(public_key_b64)
    public_keys_cache[node_id] = public_key
    return public_key

def canonical_json(obj):
    return json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

def insert_nonce(payload):
    payload["node_id"] = id
    payload["nonce"] = secrets.token_urlsafe(16) # add ~128 bits of entropy
    return payload

def sign(payload):
    signed = private_key.sign(canonical_json(payload)).signature
    return base64.urlsafe_b64encode(signed).decode().rstrip("=")

def verify_signature(node_id, payload, signature_b64):
    public_key = get_public_key(node_id)
    verify_key = signing.VerifyKey(public_key)
    try:
        signature_b64_padded = signature_b64 + "=" * (-len(signature_b64) % 4)
        verify_key.verify(canonical_json(payload), base64.urlsafe_b64decode(signature_b64_padded))
        return True
    except BadSignatureError:
        return False

def async_order(node_id, msg):
    def _post():
        try:
            insert_nonce(msg)
            signature = sign(msg)
            session.post(f"http://127.0.0.1:{8000+node_id}/order/{signature}", json=msg)
        except:
            pass
    executor.submit(_post)

def expected_messages(k):
    n = m + 2
    return perm(n-2, m-k)

total_expected = sum(expected_messages(k) for k in range(m+1))

def all_messages_received():
    total_received = len(received_values)
    # print(f'node={id}: {total_received}/{total_expected}')
    if total_received < total_expected:
        return False
    return True

def choice(vals):
    # print(f"choice(): {vals}")
    if len(vals) == 1:
        return next(iter(vals))
    # retreat if there are no values
    # retreat if there are more than 1 values (=the king is a traitor)
    return "retreat"

def broadcast(path, msg, signature_b64):
    for i in range(n):
        if i not in path:
            async_order(i, msg={"path": path, "msg": msg, "signature_b64": signature_b64})

def other_value(v):
    return "retreat" if v == "attack" else "attack"

def shortest_path():
    return list(min(received_values.keys(), key=len, default=None))

def process_message(path, msg):
    if "value" in msg:
        value = msg["value"]
        received_values[tuple(path)] = value
    elif "msg" in msg:
        contained_msg = msg["msg"]
        if not verify_signature(contained_msg["node_id"], contained_msg, msg["signature_b64"]):
            # print(f"Signature {signature_b64} does not match: msg")
            assert(False) # in my implementation, this should not happen
            # return 
        else:
            # print(f"Signature {msg['signature_b64'][:10]}[..] checks out for: {contained_msg}")
            process_message(path, msg["msg"])
    else:
        assert(False) # in my implementation, this should not happen

def message_cascade(msg, signature_b64):
    path = list(msg["path"])
    k = 1 + m - len(path)
    if k > 0:
        broadcast(path + [id], msg, signature_b64)

@app.route("/public_key")
def public_key():
    return jsonify({"node_id": id, "public_key": base64.b64encode(bytes(private_key.verify_key)).decode()})

@app.route("/order/<signature_b64>", methods=["POST"])
def order(signature_b64):
    msg = request.get_json()
    if not verify_signature(msg["node_id"], msg, signature_b64):
        # print(f"Signature {signature_b64} does not match: msg")
        return "Signature does not match", 401
    # print(f"Signature {signature_b64[:10]}[..] checks out for: {msg}")
    process_message(msg["path"], msg)
    message_cascade(msg, signature_b64)
    if all_messages_received():
        global done
        done = True
    return jsonify(ok=True)

@app.route("/decide", methods=["POST"])
def decide():
    assert(done)
    global value
    if value is None:
        value = choice(set(received_values.values()))
    return jsonify(value=value)

@app.route("/start", methods=["POST"])
def start():
    # /start is essentially like order with: path=[], value=order, k=0
    global value
    value = request.get_json()["order"]
    for i in range(n):
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
