from collections import Counter, defaultdict
from math import perm

class ByzantineConsensus:
    def __init__(self, node_id, n, m,
                 majority_func,     # majority_func(iterable[str]) -> str
                 send_func,         # send_func(target_id: int, msg: dict) -> None
                 next_value_func):  # next_value_func(v: str) -> str   
        self.id              = node_id
        self.n               = n
        self.m               = m
        self._majority       = majority_func
        self._send           = send_func
        self._next_value     = next_value_func 
        # state
        self.received_values = {}          # path-tuple -> value
        self._done           = False       # message cascade finished?
        self._value          = None
        # pre-compute expectation
        self._total_expected = sum(self._expected_messages(k) for k in range(m + 1))

    def all_messages_received(self):
        return len(self.received_values) >= self._total_expected

    def is_done(self):
        return self._done

    def value(self):
        return self._value

    def onmessage(self, msg):
        if self._done:
            raise RuntimeError("node already finished this round")
        path  = tuple(msg["path"])
        value = msg["value"]
        k = 1 + self.m - len(path)
        self.received_values[path] = value
        # forward if required
        if k > 0: # more stages to go
            value = self._next_value(value)
            if value is not None:
                self._broadcast(path + (self.id,),
                                value,
                                k - 1)
        # check completion
        if self.all_messages_received():
            self._done = True

    def decide(self, timeout_default=None):
        if self._value is not None:
            return self._value
        if not self._done and timeout_default is None:
            raise RuntimeError("cannot decide before cascade finishes and without default value")
        if len(self.received_values) == 0:
            return timeout_default
        root = min(self.received_values.keys(), key=len) # shortest path
        root = [root[0]]
        self._value = self._om(list(root), timeout_default)
        return self._value

    def start(self, initial_value: str) -> None:
        self._value = initial_value
        self._broadcast((self.id,), initial_value, self.m)
        self._done = True # king never receives later messages

    def _broadcast(self, path, value, k_remaining):
        for i in range(self.n):
            if i in path or i == self.id: # do not resend along path
                continue
            self._send(i, {"path": list(path), "value": value})

    def _expected_messages(self, k):
        return perm(3 * self.m + 1 - 2, self.m - k)

    def _om(self, path, default_value):
        k = 1 + self.m - len(path)
        v = self.received_values.get(tuple(path), default_value)
        if k == 0:
            return v
        child_vals = [self._om(path + [i], default_value)
                      for i in range(self.n)
                      if i not in path + [self.id]]
        return self._majority([v] + child_vals)
