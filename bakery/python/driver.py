import subprocess, requests, time, sys, logging
from multiprocessing import Process

logging.getLogger("werkzeug").setLevel(logging.ERROR)

num_workers = 8
num_loops   = 100

if len(sys.argv) >= 2:
    num_workers = int(sys.argv[1])
if len(sys.argv) >= 3:
    num_loops = int(sys.argv[2])

def spawn(mod, *args):
    return subprocess.Popen([sys.executable, mod, *map(str, args)])

def wait_done(i):
    url = f"http://127.0.0.1:{7000+i}/status"
    while not requests.get(url).json().get("done"):
        time.sleep(0.1)

def main():
    # start increment server
    procs = [spawn("inc_server.py")]
    # start workers
    for i in range(1, num_workers+1):
        procs.append(spawn("worker.py", num_loops, i, num_workers))
    # allow ports to open
    time.sleep(1)
    # start workers
    for i in range(1, num_workers+1):
        requests.post(f"http://127.0.0.1:{7000+i}/start")
    # wait until all workers have done their part
    for i in range(1, num_workers+1):
        wait_done(i)
    # check results
    expected = num_workers * num_loops
    observed = requests.get("http://127.0.0.1:7000/get").json()["value"]
    print(f"Workers:    {num_workers}")
    print(f"Iterations: {num_loops}")
    print(f"Expected:   {expected}")
    print(f"Observed:   {observed}")
    print(f"Passed!" if expected == observed else "FAILED!")
    # clean up
    for p in procs:
        p.terminate()
    for p in procs:
        p.wait()

if __name__ == "__main__":
    main()
