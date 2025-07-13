import sys, time, threading, requests, logging
from flask import Flask, jsonify, request

logging.getLogger("werkzeug").setLevel(logging.ERROR)

num_loops   = int(sys.argv[1])  # e.g. 1_000
my_id       = int(sys.argv[2])  # 1 .. n
num_workers = int(sys.argv[3])  # e.g. 8

app = Flask(__name__)
choosing = 0
ticket   = 0
done     = False

@app.route("/choosing")
def endpoint_choosing():
    return jsonify(choosing=choosing)

@app.route("/ticket")
def endpoint_ticket():
    return jsonify(ticket=ticket)

@app.route("/status")
def endpoint_status():
    return jsonify(done=done)

@app.route("/start", methods=["POST"])
def endpoint_start():
    threading.Thread(target=run_worker, daemon=True).start()
    return jsonify(started=True)

def worker_ticket(i):
    return requests.get(f"http://127.0.0.1:{7000+i}/ticket").json().get("ticket", 0)

def worker_choose(i):
    return requests.get(f"http://127.0.0.1:{7000+i}/choosing").json().get("choosing",0)

def announce_intent():
    global choosing, ticket
    choosing = 1
    # pick ticket = 1 + max(number)
    ticket = 1 + max(worker_ticket(i) for i in range(1, num_workers+1))
    choosing = 0

def wait_acquire():
    for j in range(1, num_workers+1):
        if j == my_id:
            continue
        while worker_choose(j):
            time.sleep(0.001)
        while True:
            tj = worker_ticket(j)
            if tj == 0 or (tj, j) > (ticket, my_id):
                break
            time.sleep(0.001)

def lock():
    announce_intent()
    wait_acquire()

def unlock():
    global ticket
    ticket = 0

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
