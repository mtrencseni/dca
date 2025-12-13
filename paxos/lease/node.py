import sys
import json
import time
import requests
import threading
from types import SimpleNamespace
from flask import Flask, request, jsonify, Response

# start nodes:
# python3 node.py 0 3
# python3 node.py 1 3
# python3 node.py 2 3
# try to acquire a lease for 5 seconds:
# curl -X POST http://localhost:5000/start

if len(sys.argv) != 3:
    print("Usage: node.py <id> <n>")
    sys.exit(1)

id, n = map(int, sys.argv[1:])
port = 5000 + id
peers = [f"http://localhost:{5000+i}" for i in range(n)]
n_majority = n//2 + 1
# globally known maximal lease time M
LEASE_SECONDS = 5.0

app = Flask(__name__)

class PaxosLeaseAcceptor:
    def __init__(self):
        self._lock = threading.Lock()
        self.state = SimpleNamespace()
        self.state.promised_n = None
        self.state.accepted_n = None
        self.state.accepted_value = None
        self._lease_timer = None  # not exposed in state/__dict__

    def _restart_timer(self, lease_seconds):
        if self._lease_timer is not None:
            self._lease_timer.cancel()
        self._lease_timer = threading.Timer(lease_seconds, self._on_timeout)
        self._lease_timer.daemon = True
        self._lease_timer.start()

    def _on_timeout(self):
        with self._lock:
            self.state.accepted_n = None
            self.state.accepted_value = None
            self._lease_timer = None

    def on_prepare(self, proposal_id):
        with self._lock:
            if self.state.promised_n is None or proposal_id > self.state.promised_n:
                self.state.promised_n = proposal_id
                success = True
            else:
                success = False
            # state.accepted_value may be None (empty) or a current lease
            return success, self.state

    def on_propose(self, proposal_id, lease_owner, lease_seconds):
        with self._lock:
            if self.state.promised_n is None or proposal_id >= self.state.promised_n:
                # accept the proposal
                self.state.promised_n = proposal_id
                self.state.accepted_n = proposal_id
                self.state.accepted_value = {
                    "owner": lease_owner,
                    "lease_seconds": lease_seconds,
                    "lease_expires_at": time.time() + lease_seconds,
                }
                self._restart_timer(lease_seconds)
                success = True
            else:
                success = False
            return success, self.state

class PaxosLeaseProposer:
    def __init__(self, node_id, peers):
        self._lock = threading.Lock()
        self.node_id = node_id
        self.peers = peers
        self.state = SimpleNamespace()
        self.state.proposal_id = self.node_id  # for uniqueness
        self.state.lease_owner = False
        self.state.lease_expires_at = None
        self._lease_timer = None

    def increment_proposal_id(self):
        with self._lock:
            # space proposal IDs by 256 to leave room for other nodes' IDs
            self.state.proposal_id += 256

    def _next_proposal_id_after(self, max_seen):
        # given the highest proposal id we've seen in the system (max_seen),
        # compute the next proposal id that belongs to this node (node_id + k*256)
        # and is strictly greater than max_seen.
        if max_seen is None:
            return self.node_id
        # proposal ids for this node look like: node_id, node_id+256, node_id+512, ...
        k = (max_seen - self.node_id) // 256 + 1
        new_id = k * 256 + self.node_id
        if new_id <= max_seen:
            new_id += 256  # safety belt; should not really happen
        return new_id

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

    def _send_propose(self, proposal_id, lease_seconds):
        return self._send_message(
            "/propose",
            {
                "proposal_id": proposal_id,
                "lease_owner": self.node_id,
                "lease_seconds": lease_seconds,
            },
        )

    def _cancel_local_lease_timer(self):
        if self._lease_timer is not None:
            self._lease_timer.cancel()
            self._lease_timer = None

    def _on_local_lease_timeout(self):
        with self._lock:
            print(f'Lease expired')
            self.state.lease_owner = False
            self.state.lease_expires_at = None
            self._lease_timer = None

    def _start_local_lease_timer(self, lease_seconds):
        self._cancel_local_lease_timer()
        self.state.lease_expires_at = time.time() + lease_seconds
        self._lease_timer = threading.Timer(lease_seconds, self._on_local_lease_timeout)
        self._lease_timer.daemon = True
        self._lease_timer.start()

    def acquire_lease(self, retry_on_prepare_fail=True):
        lease_seconds = LEASE_SECONDS
        self.increment_proposal_id()
        # Phase 1: prepare
        prepare_responses = self._send_prepare(self.state.proposal_id)
        promises = [r for r in prepare_responses if r.get("success")]
        if len(promises) < n_majority:
            # prepare failed: compute the highest proposal id we've seen
            max_seen = None
            for r in prepare_responses:
                acc_state = r.get("acceptor_state") or {}
                promised_n = acc_state.get("promised_n")
                if promised_n is not None:
                    if max_seen is None or promised_n > max_seen:
                        max_seen = promised_n
            # bump our proposal id to be > any seen, respecting node_id + k*256 layout
            if max_seen is not None:
                new_id = self._next_proposal_id_after(max_seen)
                with self._lock:
                    self.state.proposal_id = new_id
            # retry once with the new proposal id (if we haven't retried yet)
            if retry_on_prepare_fail:
                return self.acquire_lease(retry_on_prepare_fail=False) # retry only once
            # this is already the second try, just fail
            return {
                "status": "failed_prepare",
                "reason": f"Only got {len(promises)} promises, need {n_majority}",
                "proposal_id": self.state.proposal_id,
                "prepare_responses": prepare_responses,
            }
        # in PaxosLease, we may only propose ourselves if a majority
        # returned "empty" accepted proposals, i.e. no current lease
        open_promises = []
        for r in promises:
            acc_state = r.get("acceptor_state", {})
            accepted_value = acc_state.get("accepted_value")
            if accepted_value is None:
                open_promises.append(r)
        if len(open_promises) < n_majority:
            return {
                "status": "lease_busy",
                "reason": (
                    "A majority of acceptors already hold some lease; "
                    "cannot safely acquire a new one now."
                ),
                "proposal_id": self.state.proposal_id,
                "prepare_responses": prepare_responses,
            }
        # Phase 2: propose ourselves as lease owner
        # per PaxosLease, we start our local timer BEFORE sending propose requests
        self._start_local_lease_timer(lease_seconds)
        propose_responses = self._send_propose(self.state.proposal_id, lease_seconds)
        accepts = [r for r in propose_responses if r.get("success")]
        if len(accepts) < n_majority:
            # failed to get a majority; cancel our local lease timer
            self._cancel_local_lease_timer()
            with self._lock:
                self.state.lease_owner = False
                self.state.lease_expires_at = None
            return {
                "status": "failed_propose",
                "reason": f"Only got {len(accepts)} accepts, need {n_majority}",
                "proposal_id": self.state.proposal_id,
                "lease_seconds": lease_seconds,
                "prepare_responses": prepare_responses,
                "propose_responses": propose_responses,
            }
        # success: we now believe we have the lease until our local timer fires.
        with self._lock:
            self.state.lease_owner = True
            print(f'I am the lease owner for {lease_seconds} seconds')
        # we do NOT need to broadcast learn; other nodes can't reliably
        # know the remaining lease time due to network delay.
        return {
            "status": "success",
            "proposal_id": self.state.proposal_id,
            "lease_owner": self.node_id,
            "lease_seconds": lease_seconds,
            "lease_expires_at": self.state.lease_expires_at,
            "prepare_responses": prepare_responses,
            "propose_responses": propose_responses,
        }

acceptor = PaxosLeaseAcceptor()
proposer = PaxosLeaseProposer(id, peers)

@app.route("/start", methods=["POST"])
def start():
    result = proposer.acquire_lease()
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
    lease_owner = data.get("lease_owner")
    lease_seconds = data.get("lease_seconds")
    if proposal_id is None or lease_owner is None or lease_seconds is None:
        return jsonify({
            "ok": False,
            "error": "missing proposal_id, lease_owner or lease_seconds"
        }), 400
    success, state = acceptor.on_propose(proposal_id, lease_owner, float(lease_seconds))
    return jsonify({
        "success": success,
        "acceptor_state": state.__dict__,
    })

@app.route("/status", methods=["GET"])
def status():
    payload = {
        "node_id": id,
        "proposer_state": proposer.state.__dict__,
        "acceptor_state": acceptor.state.__dict__,
    }
    return Response(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        mimetype="application/json"
    )

if __name__ == "__main__":
    print(f"Node {id} starting, waiting for {LEASE_SECONDS} seconds to respect PaxosLease protocol...")
    time.sleep(LEASE_SECONDS)
    print(f"Node {id} is now active.")
    app.run(host="0.0.0.0", port=port, debug=False)
