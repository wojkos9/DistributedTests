from utils import *
from threading import Thread, Lock, Semaphore
from collections import deque
from itertools import chain
import time

class WorkerPool():
    def __init__(self, counts, debug, classmap):
        self.counts = counts
        self.debug = debug if debug else DEBUG10
        self.size = sum(counts)
        self.queues = [deque() for _ in range(self.size)]
        self.msems = [Semaphore(0) for _ in range(self.size)]
        self.types = list(classmap.keys())
        self.typemap = list(chain.from_iterable(
            [[typ] * count for typ, count in zip(self.types, self.counts)]))

        self.energy = counts[2]
        self.energy_max = self.energy

        params = {"pool": self, "debug": debug, "ene": self.energy}
        self.threads = [classmap[self.typemap[i]](i, **params) for i in range(self.size)]
        self.pid = 999
        
        self.loc = Lock()

        # Thread(target=self.rep_th, daemon=True).start()
    
    def send(self, tid, m):
        self.queues[tid].append(m)
        self.msems[tid].release()

    def dec(self):
        # self.loc.acquire() # prob unneeded
        self.energy -= 1
        # self.loc.release()

    def get_of_type(self, typ):
        return filter(lambda i: self.threads[i].typ==typ, range(self.size))
    
    def id_in_group(self, pid):
        c = self.counts
        return  pid if pid < c[0] else (
                pid-c[0] if pid < c[0]+c[1] else 
                pid-c[0]-c[1])

    def do_report(self):
        try:
            ts = self.threads
            msg = "-"*48 + "\n"
            msg += " ".join("%3d"%(t.last_crit) for t in ts) + "\n"
            check_cons = all(ts[t.pair].pair in (i, -1) for i,t in enumerate(ts) if t.pair != -1 and t.state > ST.WAIT)
            msg += " ".join(f"{min(t.pid, t.pair)}-{max(t.pid, t.pair)}" if t.pair != -1 else "---" for t in ts)
            msg += " | " + " ".join([str(t.ack_count) for t in ts[:self.counts[0]]])
            msg += "  OK" if check_cons else " ERR" # nie zawsze ok w kolejnych it. bo nie synchronizują pracy
            msg += "\n" + " ".join("%3s"%(t.state.name[0]) for t in ts) + " | "+" ".join([str(t.energy) for t in ts[:self.counts[0]]]) + f" | {self.energy}"
            print(msg)
            if self.energy > self.energy_max:
                raise Exception("ENERGY ABOVE MAX")
            elif self.energy < 0:
                raise Exception("ENERGY BELOW 0")
        except Exception as e:
            time.sleep(1)
            print(e)
            exit(-1)
    
    def rep_th(self):
        while 1:
            self.do_report()
            time.sleep(1)

    def desc(self, pid):
        return self.threads[pid].desc if pid != self.pid else "POOL"
    
    def start(self):
        for t in self.threads:
            t.start()
    
    def join(self):
        for t in self.threads:
            t.join()

    def force_join(self):
        for pid in range(self.size):
            self.send(pid, TMsg(MTyp.TER, self.pid, {}, 0))
        self.join()