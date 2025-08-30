import subprocess, sys, random, time, requests

n = int(sys.argv[1]) if len(sys.argv) >= 2 else 4
m = int(sys.argv[2]) if len(sys.argv) >= 3 else n-2

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
# kick off message cascade by telling the commander to issue his order
requests.post("http://127.0.0.1:8000/start", json={"order": "attack" if random.random() < 0.5 else "retreat"})
# wait until all generals report done
while True:
    time.sleep(0.1)
    try:
        if all(requests.get(f"http://127.0.0.1:{8000+i}/status").json()["done"] for i in range(n)):
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
        value = requests.post(f"http://127.0.0.1:{8000+i}/decide").json()["value"]
        print(f'General {i} decided {value}')
        decisions.add(value)
if len(decisions) == 1:
    print(f'SUCCESS: all non-traitor generals decided to {value}!')
else:
    print('FAILURE: non-traitor generals decided differently')
# stop all generals
for p in procs:
    p.kill()
