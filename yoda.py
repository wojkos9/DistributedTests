from threading import Thread, Semaphore, Lock
from collections import deque
import heapq
from dataclasses import dataclass
from enum import Enum
import time
import random
import sys



class MTyp(Enum):
    DEF = 0
    FIN = 1
    REQ = 2
    ACK = 3
    REL = 4

class St(Enum):
    IDLE = 0
    WAIT = 1
    CRIT = 2

@dataclass
class TMsg:
    typ: MTyp
    sender: int
    data: dict
    cl: int

def randintnot(a, b, n):
    return random.randint(*[(a, n-1), (n+1, b)][random.random()>(n-a)/(b-a)])

def debug(*args, **kwargs):
    if "lvl" in kwargs:
        lvl = kwargs["lvl"]
        del kwargs["lvl"]
    else:
        lvl = 1
    if lvl <= DEBUG_LVL:
        print(*args, **kwargs)

DEBUG_LVL = 1

class CSAlg(Enum):
    LAMPORT = 0
    RIC_AGR = 1

class Worker(Thread):
    def __init__(self, id, size, pool, sync=None):
        Thread.__init__(self, daemon=True)
        self.id = id
        self.size = size
        self.pool = pool
        self.lclock = 0
        self.qu = pool.qu[id]
        self.procqu = []
        self.status = St.IDLE
        self.sem = pool.sem[self.id]
        self.crit_lock = Lock()
        self.crit_lock.acquire()
        self.sync = sync
        self.ricagr_count = 0
        self.ricagr_req = None

    def debug(self, *args, **kwargs):
        debug("[{}]".format(self.id), *args, **kwargs)

    def send(self, tid, typ=MTyp.DEF, data={}):
        self.lclock += 1
        self.pool.qu[tid].append(TMsg(typ, self.id, data, self.lclock))
        self.pool.sem[tid].release()

    def _recv(self) -> TMsg:
        q = self.qu
        if q:
            m = q.popleft()
            lclock = max(self.lclock, m.cl) + 1

            prio = 2 if m.typ != MTyp.DEF else 3
            self.debug("[<-{}-{}] [cl {}->{}] RECV: cl={}, data=\"{}\""
                .format(m.typ.name, m.sender, self.lclock, lclock, m.cl, m.data), lvl=prio)
            
            self.lclock = lclock
            return m
        else:
            return None

    def critical_section(self, sect_time=2):
        global all_th
        self.status = St.CRIT
        debug("---------CRIT  {}----------<".format(self.id))

        for i in range(sect_time):
            self.debug("[msg {}/{}] IN CRIT:".format(i+1, sect_time), self.pool.status_report(St.CRIT),
                "WAIT:", self.pool.status_report(St.WAIT), lvl=1)
            time.sleep(1)

        debug("---------LEAVE {}---------->".format(self.id))
        self.status = St.IDLE

    def queue_remove_entry_for(self, sender):
        heap = self.procqu
        reqi = next(filter(lambda i: heap[i][1]==sender, range(0, len(heap))), -1)
        if reqi == -1:
            raise Exception("Request not in list")
        else:
            heap[reqi] = self.procqu[-1]
            heap.pop()
            heapq.heapify(self.procqu)

    def ricagr_try(self):
        if self.status == St.IDLE:
            debug("---------TRY   {}----------?".format(self.id))
            self.status = St.WAIT
            self.ricagr_req = self.lclock
            self.ricagr_count = 0

            data = {"prio": self.ricagr_req}
            for i in range(self.size):
                if i != self.id:
                    self.send(i, typ=MTyp.REQ, data=data)
            
            return True
        return False
    
    def ricagr_recv(self):
        m = self._recv()
        if m:
            if m.typ == MTyp.REQ:
                prio = m.data["prio"]
                if self.status != St.IDLE and (self.ricagr_req, self.id) < (prio, m.sender):
                        heapq.heappush(self.procqu, (prio, m.sender))
                else:
                    self.send(m.sender, typ=MTyp.ACK)
                
            elif m.typ == MTyp.ACK:
                if self.status == St.WAIT:
                    self.ricagr_count += 1
                    if self.ricagr_count == self.size-1:
                        self.debug("----CAN ENTER----", lvl=2)
                        if self.crit_lock.locked():
                            self.crit_lock.release()

    def ricagr_post(self):
        for _, tid in heapq.nsmallest(len(self.procqu), self.procqu):
            self.send(tid, typ=MTyp.ACK)
        self.ricagr_count = 0
        self.procqu = []
    
    def comm_thread(self, recv_f):
        self.debug("START COMM", lvl=2)
        while 1:
            self.sem.acquire()
            recv_f()

    def run(self):
        if self.sync:
            self.sync.acquire()
        recv_f, try_f, post_f = self.ricagr_recv, self.ricagr_try, self.ricagr_post
        self.debug("START", lvl=2)
        com_th = Thread(target=self.comm_thread, daemon=True, args=[recv_f])
        com_th.start()
        while 1:
            if random.random() < 1/self.size:
                if try_f():
                    debug("---------WAIT  {}----------|".format(self.id))
                    self.crit_lock.acquire()
                    self.critical_section()
                    if post_f:
                        post_f()
            time.sleep(2*random.random())
        self.debug("FIN", lvl=2)


class WorkerPool:
    def __init__(self, count, has_sync=True):
        self.count = count
        self.qu: list[TMsg] = [deque() for _ in range(count)]
        self.sem: list[Semaphore] = [Semaphore(value=0) for _ in range(count)]
        self.all_th = []
        self.sync = Semaphore(value=0) if has_sync else None
    
    def spawn_threads(self):
        self.all_th = []
        for id in range(self.count):
            t = Worker(id, self.count, self, self.sync)
            t.start()
            self.all_th.append(t)

    def status_report(self, comp=None):
        if not comp:
            return " ".join([str(i) + ": {}".format(t.status.name) for i, t in enumerate(self.all_th[:self.count])])
        else:
            return " ".join([(str(i) if t.status==comp else "_") for i, t in enumerate(self.all_th[:self.count])])

    def start_pool(self, quiet=False):
        self.sync.release(self.count)
        if not quiet:
            print(self.status_report())

    def _mon_fun(self):
        while 1:
            stat = self.status_report()
            retl_expr = "\r"
            sys.stdout.write(stat+retl_expr)
            time.sleep(1)

    def start_monitor(self, ivl=1):
        global DEBUG_LVL
        DEBUG_LVL = 0
        t = Thread(target=self._mon_fun)
        self.all_th.append(t)
        t.start()

    def join(self):
        for t in self.all_th:
            t.join()

if __name__=="__main__":
    pool = WorkerPool(5)

    DEBUG_LVL = 1
    pool.spawn_threads()
    
    # pool.start_monitor()
    pool.start_pool()

    pool.join()
