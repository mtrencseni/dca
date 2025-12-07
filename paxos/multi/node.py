import sys
import time
import json
import requests
import threading
from types import SimpleNamespace
from flask import Flask, request, jsonify, Response

# start nodes:
# python3 node.py 0 3
# python3 node.py 1 3
# python3 node.py 2 3
#
# send a command:
# curl -X POST http://localhost:5000/command -H "Content-Type: application/json" -d '{"command": "li = []"}'
# curl -X POST http://localhost:5000/command -H "Content-Type: application/json" -d '{"command": "li += [1, 2]"}'
# curl -X POST http://localhost:5000/command -H "Content-Type: application/json" -d '{"command": "li += [3, 4]"}'
# curl -X POST http://localhost:5000/command -H "Content-Type: application/json" -d '{"command": "i = 0"}'
# curl -X POST http://localhost:5002/command -H "Content-Type: application/json" -d '{"command": "i = 42"}'
# curl http://localhost:5000/db

if len(sys.argv) != 3:
    print("Usage: node2.py <id> <n>")
    sys.exit(1)

node_id, n = map(int, sys.argv[1:])
port = 5000 + node_id
peers = [f"http://localhost:{5000 + i}" for i in range(n)]
n_majority = n // 2 + 1
# current round (next slot to propose into)
current_round = 0
round_lock = threading.Lock()
# simple per-node "database": a dict that exec commands write into
db = {}
db_lock = threading.Lock()

app = Flask(__name__)

def get_current_round():
    with round_lock:
        return current_round

def advance_round(new_round):
    global current_round
    with round_lock:
        if new_round > current_round:
            current_round = new_round

class PaxosAcceptor:
    def __init__(self):
        self._lock = threading.Lock()
        self.rounds = {}  # round_id -> SimpleNamespace(promised_n, accepted_n, accepted_value)

    def _get_round_state(self, round_id):
        if round_id not in self.rounds:
            self.rounds[round_id] = SimpleNamespace(
                promised_n=None,
                accepted_n=None,
                accepted_value=None,
            )
        return self.rounds[round_id]

    def on_prepare(self, round_id, proposal_id):
        with self._lock:
            st = self._get_round_state(round_id)
            if st.promised_n is None or proposal_id > st.promised_n:
                st.promised_n = proposal_id
                success = True
            else:
                success = False
            return success, st

    def on_propose(self, round_id, proposal_id, value):
        with self._lock:
            st = self._get_round_state(round_id)
            if st.promised_n is None or proposal_id >= st.promised_n:
                st.promised_n = proposal_id
                st.accepted_n = proposal_id
                st.accepted_value = value
                success = True
            else:
                success = False
            return success, st

class PaxosLearner:
    def __init__(self, db, db_lock):
        self._lock = threading.Lock()
        self.rounds = {}  # round_id -> SimpleNamespace(chosen_value)
        self.db = db
        self.db_lock = db_lock

    def _get_round_state(self, round_id):
        if round_id not in self.rounds:
            self.rounds[round_id] = SimpleNamespace(chosen_value=None)
        return self.rounds[round_id]

    def learn(self, round_id, command_str):
        with self._lock:
            st = self._get_round_state(round_id)
            # Paxos should never learn two different values for the same round
            if st.chosen_value is not None:
                assert st.chosen_value == command_str
                return True, st
            st.chosen_value = command_str
        # apply the command to the local "database"
        # NOTE: this uses exec and is obviously unsafe in real life.
        with self.db_lock:
            exec(command_str, {}, self.db)
        return True, st

class PaxosProposer:
    def __init__(self, node_id, peers):
        self._lock = threading.Lock()
        self.node_id = node_id
        self.peers = peers
        self.state = SimpleNamespace()
        self.state.proposal_id = self.node_id  # used to generate unique proposal IDs

    def increment_proposal_id(self):
        with self._lock:
            self.state.proposal_id += 256

    def _http_post_json(self, url, path, payload):
        try:
            resp = requests.post(f"{url}{path}", json=payload, timeout=1.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def _send_message(self, endpoint, message):
        responses = []
        for peer in self.peers:
            data = self._http_post_json(peer, endpoint, message)
            if data is not None:
                data["node"] = peer
                responses.append(data)
        return responses

    def _send_prepare(self, round_id, proposal_id):
        return self._send_message("/prepare", {"round_id": round_id, "proposal_id": proposal_id})

    def _send_propose(self, round_id, proposal_id, value):
        return self._send_message("/propose", {"round_id": round_id, "proposal_id": proposal_id, "value": value})

    def _broadcast_learn(self, round_id, value):
        for peer in self.peers:
            self._http_post_json(peer, "/learn", {"round_id": round_id, "value": value})

    def paxos_round(self, round_id, initial_value):
        self.increment_proposal_id()
        pid = self.state.proposal_id
        # phase 1: prepare
        prepare_responses = self._send_prepare(round_id, pid)
        promises = [r for r in prepare_responses if r.get("success")]
        if len(promises) < n_majority:
            return {
                "status": "failed_prepare",
                "reason": f"Only got {len(promises)} promises, need {n_majority}",
                "round_id": round_id,
                "proposal_id": pid,
                "prepare_responses": prepare_responses,
            }
        # if any acceptor already accepted a value, choose the one with the highest accepted_n
        chosen_value = initial_value
        highest_accepted_n = -1
        for r in promises:
            st = r.get("acceptor_state", {})
            accepted_n = st.get("accepted_n")
            accepted_value = st.get("accepted_value")
            if accepted_n is not None and accepted_value is not None and accepted_n > highest_accepted_n:
                highest_accepted_n = accepted_n
                chosen_value = accepted_value
        # phase 2: propose
        propose_responses = self._send_propose(round_id, pid, chosen_value)
        accepts = [r for r in propose_responses if r.get("success")]
        if len(accepts) < n_majority:
            return {
                "status": "failed_propose",
                "reason": f"Only got {len(accepts)} accepts, need {n_majority}",
                "round_id": round_id,
                "proposal_id": pid,
                "value": chosen_value,
                "prepare_responses": prepare_responses,
                "propose_responses": propose_responses,
            }
        # phase 3: learn
        self._broadcast_learn(round_id, chosen_value)
        return {
            "status": "success",
            "round_id": round_id,
            "proposal_id": pid,
            "value": chosen_value,
            "prepare_responses": prepare_responses,
            "propose_responses": propose_responses,
        }

acceptor = PaxosAcceptor()
learner = PaxosLearner(db, db_lock)
proposer = PaxosProposer(node_id, peers)

@app.route("/command", methods=["POST"])
def endpoint_command():
    data = request.get_json(force=True, silent=True) or {}
    if "command" not in data:
        return jsonify({"error": "Missing 'command' in JSON body"}), 400
    round_id = get_current_round()
    result = proposer.paxos_round(round_id, data["command"])
    if result.get("status") == "success":
        # advance to next round once this one is chosen
        advance_round(round_id + 1)
    return jsonify(result)

@app.route("/prepare", methods=["POST"])
def endpoint_prepare():
    data = request.get_json(force=True, silent=True) or {}
    round_id = data.get("round_id")
    proposal_id = data.get("proposal_id")
    if round_id is None or proposal_id is None:
        return jsonify({"success": False, "error": "missing round_id or proposal_id"}), 400
    success, state = acceptor.on_prepare(round_id, proposal_id)
    return jsonify({
        "success": success,
        "acceptor_state": state.__dict__,
    })

@app.route("/propose", methods=["POST"])
def endpoint_propose():
    data = request.get_json(force=True, silent=True) or {}
    round_id = data.get("round_id")
    proposal_id = data.get("proposal_id")
    if round_id is None or proposal_id is None or "value" not in data:
        return jsonify({"success": False, "error": "missing round_id, proposal_id or value"}), 400
    success, state = acceptor.on_propose(round_id, proposal_id, data["value"])
    return jsonify({
        "success": success,
        "acceptor_state": state.__dict__,
    })

@app.route("/learn", methods=["POST"])
def endpoint_learn():
    data = request.get_json(force=True, silent=True) or {}
    round_id = data.get("round_id")
    if round_id is None or "value" not in data:
        return jsonify({"error": "missing round_id or value"}), 400
    success, state = learner.learn(round_id, data["value"])
    return jsonify({
        "success": success,
        "learner_state": state.__dict__,
    })


@app.route("/current", methods=["GET"])
def endpoint_current():
    return jsonify({
        "round_id": get_current_round(),
    })

@app.route("/fetch", methods=["GET"])
def endpoint_fetch():
    try:
        round_id = int(request.args.get("round_id"))
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "missing or invalid round_id"}), 400
    st = learner.rounds.get(round_id)
    if st is None or st.chosen_value is None:
        return jsonify({"success": False, "error": "no value for this round"}), 404
    return jsonify({
        "success": True,
        "round_id": round_id,
        "value": st.chosen_value,
    })

@app.route("/status", methods=["GET"])
def endpoint_status():
    # shallow snapshot
    with db_lock:
        db_copy = dict(db)
    payload = {
        "node_id": node_id,
        "current_round": get_current_round(),
        "db": db_copy,
        "proposer_state": proposer.state.__dict__,
        "acceptor_state": {rid: st.__dict__ for rid, st in acceptor.rounds.items()},
        "learner_state": {rid: st.__dict__ for rid, st in learner.rounds.items()},
    }
    return Response(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        mimetype="application/json"
    )


@app.route("/db", methods=["GET"])
def endpoint_db():
    # shallow snapshot
    with db_lock:
        db_copy = dict(db)
    payload = {
        "current_round": get_current_round(),
        "db": db_copy,
    }
    return Response(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        mimetype="application/json"
    )

def try_catchup():
    # Background loop: poll peers' /current and, if they are ahead, pull
    # missing rounds via /fetch and apply them locally via learner.learn()
    while True:
        time.sleep(1.0)
        local_round = get_current_round()
        for peer in peers:
            # don't query self
            if peer.endswith(str(port)):
                continue
            try:
                r = requests.get(f"{peer}/current", timeout=1.0)
                if r.status_code != 200:
                    continue
                peer_round = r.json().get("round_id", 0)
            except Exception:
                continue
            if peer_round > local_round:
                for rid in range(local_round, peer_round):
                    try:
                        resp = requests.get(f"{peer}/fetch", params={"round_id": rid}, timeout=1.0)
                        if resp.status_code != 200:
                            continue
                        data = resp.json()
                        if not data.get("success"):
                            continue
                        value = data.get("value")
                        learner.learn(rid, value)
                    except Exception:
                        continue
                advance_round(peer_round)
                local_round = peer_round

if __name__ == "__main__":
    # start background sync thread
    t = threading.Thread(target=try_catchup, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=port, debug=False)
