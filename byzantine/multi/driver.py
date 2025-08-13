import subprocess, sys, random, time, requests

if len(sys.argv) != 3:
    print("Usage: general.py <m> <num_rounds>"); sys.exit(1)

m = int(sys.argv[1])
n = 3*m + 1
num_rounds = int(sys.argv[2])
timeout = 5 # seconds

# randomly pick m traitors (could include commander 0)
traitors = set(random.sample(range(n), m))
print(f'Traitors: {traitors}')

def spawn(mod, *args):
    return subprocess.Popen([sys.executable, mod, *map(str, args)])

procs = []
# start every general
for gid in range(n):
    traitor = 1 if gid in traitors else 0
    procs.append(spawn("general.py", gid, n, m, traitor))
# let servers come up
time.sleep(1)
for round_id in range(num_rounds):
    king_id = random.sample(range(n), 1)[0]
    print(f"\nROUND #{round_id}, node {king_id} is king")
    # kick off message cascade by telling the king to issue his order
    activities = ["drink beer", "eat dinner", "sleep", "watch a movie", "go clubbing"]
    order = random.sample(activities, 1)[0]
    requests.post(f"http://127.0.0.1:{8000+king_id}/start", json={"round_id": round_id, "order": order})
    # wait until all generals report done
    timeout_ts = time.monotonic() + timeout
    while True:
        if time.monotonic() > timeout_ts:
            print("Timeout, calling decide(), generals will assume default values for missing messages")
            break
        time.sleep(0.1)
        try:
            if all(requests.get(f"http://127.0.0.1:{8000+i}/status", json={"round_id": round_id}).json()["done"] for i in range(n)):
                break # all done, exit waiting loop
        except:
            pass
    # tell each general to decide based on what they've seen so far
    print('Decisions:')
    decisions = set()
    for i in range(n):
        if i in traitors:
            print(f'General {i} is a traitor')
        else:
            value = requests.post(f"http://127.0.0.1:{8000+i}/decide", json={"round_id": round_id}).json()["value"]
            print(f'General {i} decided {value}')
            decisions.add(value)
    if len(decisions) == 1:
        print(f'SUCCESS: all non-traitor generals decided to {value}!')
    else:
        print('FAILURE: non-traitor generals decided differently')
# stop all generals
for p in procs:
    p.kill()
