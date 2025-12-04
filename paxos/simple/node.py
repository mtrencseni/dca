import sys
import requests
import threading
from types import SimpleNamespace
from flask import Flask, request, jsonify

# start nodes:
# python3 node.py 0 3
# python3 node.py 1 3
# python3 node.py 2 3
# kick off a round of paxos:
# curl -X POST http://localhost:5000/start -H "Content-Type: application/json" -d '{"value": "foo"}'

if len(sys.argv) != 3:
    print("Usage: node.py <id> <n>")
    sys.exit(1)

id, n = map(int, sys.argv[1:])
port = 5000 + id
peers = [f"http://localhost:{5000+i}" for i in range(n)]
n_majority = n//2 + 1

app = Flask(__name__)

class PaxosAcceptor:
    def __init__(self):
        self._lock = threading.Lock()
        self.state = SimpleNamespace()
        self.state.promised_n = None
        self.state.accepted_n = None
        self.state.accepted_value = None

    def on_prepare(self, proposal_id):
        with self._lock:
            if self.state.promised_n is None or proposal_id > self.state.promised_n:
                self.state.promised_n = proposal_id
                success = True
            else:
                success = False
            return success, self.state

    def on_propose(self, proposal_id, value):
        with self._lock:
            if self.state.promised_n is None or proposal_id >= self.state.promised_n:
                self.state.promised_n = proposal_id
                self.state.accepted_n = proposal_id
                self.state.accepted_value = value
                success = True
            else:
                success = False
            return success, self.state

class PaxosLearner:
    def __init__(self):
        self._lock = threading.Lock()
        self.state = SimpleNamespace()
        self.state.chosen_value = None

    def learn(self, value):
        with self._lock:
            # in Paxos, the learner should never get two different chosen values
            if self.state.chosen_value is not None:
                assert(self.state.chosen_value == value)
            self.state.chosen_value = value
            return True, self.state

class PaxosProposer:
    def __init__(self, node_id, peers):
        self._lock = threading.Lock()
        self.node_id = node_id
        self.peers = peers
        self.state = SimpleNamespace()
        self.state.proposal_id = self.node_id # used to generate unique proposal IDs

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

    def _send_prepare(self, proposal_id):
        return self._send_message("/prepare", {"proposal_id": proposal_id})

    def _send_propose(self, proposal_id, value):
        return self._send_message("/propose", {"proposal_id": proposal_id, "value": value})

    def _broadcast_learn(self, value):
        for peer in self.peers:
            self._http_post_json(peer, "/learn", {"value": value})

    def paxos_round(self, initial_value):
        self.increment_proposal_id()
        # phase 1: prepare
        prepare_responses = self._send_prepare(self.state.proposal_id)
        promises = [r for r in prepare_responses if r.get("success")]
        if len(promises) < n_majority:
            return {
                "status": "failed_prepare",
                "reason": f"Only got {len(promises)} promises, need {n_majority}",
                "proposal_id": self.state.proposal_id,
                "prepare_responses": prepare_responses,
            }
        # if any acceptor already accepted a value, choose the one with the highest accepted_n
        chosen_value = initial_value
        highest_accepted_n = -1
        for r in promises:
            accepted_n = r["acceptor_state"].get("accepted_n")
            accepted_value = r["acceptor_state"].get("accepted_value")
            if accepted_n is not None and accepted_value is not None and accepted_n > highest_accepted_n:
                highest_accepted_n = accepted_n
                chosen_value = accepted_value
        # phase 2: propose
        propose_responses = self._send_propose(self.state.proposal_id, chosen_value)
        accepts = [r for r in propose_responses if r.get("success")]
        if len(accepts) < n_majority:
            return {
                "status": "failed_propose",
                "reason": f"Only got {len(accepts)} accepts, need {n_majority}",
                "proposal_id": self.state.proposal_id,
                "value": chosen_value,
                "prepare_responses": prepare_responses,
                "propose_responses": propose_responses,
            }
        # phase 3: learn
        self._broadcast_learn(chosen_value)
        return {
            "status": "success",
            "proposal_id": self.state.proposal_id,
            "value": chosen_value,
            "prepare_responses": prepare_responses,
            "propose_responses": propose_responses,
        }

acceptor = PaxosAcceptor()
learner = PaxosLearner()
proposer = PaxosProposer(id, peers)

@app.route("/start", methods=["POST"])
def start():
    data = request.get_json(force=True, silent=True) or {}
    if "value" not in data:
        return jsonify({"error": "Missing 'value' in JSON body"}), 400
    result = proposer.paxos_round(data["value"])
    return jsonify(result)

@app.route("/prepare", methods=["POST"])
def prepare():
    data = request.get_json(force=True, silent=True) or {}
    proposal_id = data.get("proposal_id")
    if proposal_id is None:
        return jsonify({"ok": False, "error": "missing proposal_id"}), 400
    success, state = acceptor.on_prepare(proposal_id)
    return jsonify({
        "success": success,
        "acceptor_state": state.__dict__,
    })

@app.route("/propose", methods=["POST"])
def propose():
    data = request.get_json(force=True, silent=True) or {}
    proposal_id = data.get("proposal_id")
    if proposal_id is None or "value" not in data:
        return jsonify({"ok": False, "error": "missing proposal_id or value"}), 400
    success, state = acceptor.on_propose(proposal_id, data["value"])
    return jsonify({
        "success": success,
        "acceptor_state": state.__dict__,
    })

@app.route("/learn", methods=["POST"])
def learn():
    data = request.get_json(force=True, silent=True) or {}
    if "value" not in data:
        return jsonify({"error": "missing value"}), 400
    success, state = learner.learn(data["value"])
    return jsonify({
        "success": success,
        "learner_state": state.__dict__,
    })

@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "node_id": id,
        "proposer_state": proposer.state.__dict__,
        "acceptor_state": acceptor.state.__dict__,
        "learner_state": learner.state.__dict__,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port, debug=False)
