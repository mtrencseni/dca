import sys, time, threading, requests, json, logging
from flask import Flask, request, jsonify

logging.getLogger("werkzeug").setLevel(logging.ERROR)

num_loops   = int(sys.argv[1])  # e.g. 1_000
my_id       = int(sys.argv[2])  # 1 .. n
num_workers = int(sys.argv[3])  # e.g. 8

app = Flask(__name__)
clock            = 0                # Lamport logical clock
requesting       = False            # am I trying to get the lock to run my critical section?
request_ts       = 0                # timestamp of my current request
replies_needed   = 0                # remaining replies to receive
deferred_replies = set()            # worker IDs whose reply is deferred
done             = False            # set to True when loops finished
guard_lock       = threading.Lock() # guards all variables above

@app.route("/request", methods=["POST"])
def endpoint_request():
    id = int(request.get_json()["id"])
    ts = int(request.get_json()["ts"])
    global clock
    with guard_lock:
        clock = max(clock, ts) + 1
        grant_request = (not requesting) or (ts, id) < (request_ts, my_id)
        if grant_request:
            # reply immediately
            requests.post(f"http://127.0.0.1:{7000+id}/reply", json={"id": my_id, "ts": clock})
        else:
            deferred_replies.add(id)
    return jsonify(ok=True)

@app.route("/reply", methods=["POST"])
def endpoint_reply():
    ts = int(request.get_json()["ts"])
    global clock, replies_needed
    with guard_lock:
        clock = max(clock, ts) + 1
        replies_needed -= 1 # we assume each worker only replies once
    return jsonify(ok=True)

@app.route("/start", methods=["POST"])
def endpoint_start():
    threading.Thread(target=run_worker, daemon=True).start()
    return jsonify(started=True)

@app.route("/status")
def endpoint_status():
    return jsonify(done=done)

def lock():
    global clock, requesting, request_ts, replies_needed
    with guard_lock:
        requesting = True
        clock += 1
        request_ts = clock
        replies_needed = num_workers - 1
    # broadcast request
    for i in range(1, num_workers + 1):
        if i != my_id:
            requests.post(f"http://127.0.0.1:{7000+i}/request", json={"ts": request_ts, "id": my_id})
    # wait for all replies
    while True:
        with guard_lock:
            if replies_needed == 0:
                break
        time.sleep(0.001)

def unlock():
    global requesting
    with guard_lock:
        pending = list(deferred_replies)
        deferred_replies.clear()
        requesting = False
    for i in pending:
        requests.post(f"http://127.0.0.1:{7000+i}/reply", json={"id": my_id, "ts": clock})

def critical_section():
    curr = requests.get(f"http://127.0.0.1:7000/get").json()["value"]     # get
    requests.post(f"http://127.0.0.1:7000/set", json={"value": curr + 1}) # set

def run_worker():
    global done
    for i in range(num_loops):
        lock()
        critical_section()
        unlock()
        #print(f"Worker {my_id} at {i}")
    done = True
    print(f"Worker {my_id} Done.", flush=True)

if __name__ == "__main__":
    app.run(port=7000+my_id, threaded=False)
