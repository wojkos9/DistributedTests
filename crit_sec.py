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
    SYN = 5
    REP = 6

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
    def __init__(self, id, size, pool, sync=None, crit_sec_alg=CSAlg.LAMPORT, is_reporter=False):
        Thread.__init__(self, daemon=True)
        self.id = id
        self.size = size
        self.pool = pool
        self.lclock = 0
        self.qu = pool.qu[id]
        self.procqu = []
        self.last_clocks = [0] * self.size
        self.status = St.IDLE
        self.sem = pool.sem[self.id]
        self.crit_lock = Lock()
        self.crit_lock.acquire()
        self.sync = sync
        self.ricagr_count = 0
        self.ricagr_req = None
        self.crit_sec_alg = crit_sec_alg
        self.units = 100
        self.is_reporter = is_reporter

    def debug(self, *args, **kwargs):
        debug("[{}] {}".format(self.id, self.crit_sec_alg.name), *args, **kwargs)

    def send(self, tid, typ=MTyp.DEF, data={}):
        self.lclock += 1
        self.pool.qu[tid].append(TMsg(typ, self.id, data, self.lclock))
        self.pool.sem[tid].release()
    
    def send_units(self, tid, count):
        self.units -= count
        time.sleep(random.random()*2)
        self.send(tid, data={"count": count})

    def _recv(self) -> TMsg:
        q = self.qu
        if q:
            m = q.popleft()
            lclock = max(self.lclock, m.cl) + 1

            prio = 2 if m.typ in (MTyp.REQ, MTyp.ACK, MTyp.REL) else 3
            if m.typ == MTyp.REP:
                prio = 0
            self.debug("[<-{}-{}] [cl {}->{}] RECV: cl={}, data=\"{}\""
                .format(m.typ.name, m.sender, self.lclock, lclock, m.cl, m.data), lvl=prio)
            
            self.lclock = lclock

            if m.typ == MTyp.REP and "units" not in m.data:
                self.send(m.sender, MTyp.REP, {"units": self.units})
            elif m.typ == MTyp.DEF:
                self.units += m.data["count"]

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

    def lamport_can_enter(self):
        req = heapq.nsmallest(1, self.procqu)
        if req:
            req = req[0]
            if req[1] == self.id and all(lc > req[0] for lc in self.last_clocks):
                return True
        return False

    def lamport_try(self):
        if self.status == St.IDLE:
            debug("---------TRY   {}----------?".format(self.id))
            self.status = St.WAIT
            for i in range(self.size):
                self.send(i, typ=MTyp.REQ)
            return True
        return False
        
    def lamport_recv(self):
        m = self._recv()
        if m:
            self.last_clocks[m.sender] = self.lclock
            if m.typ == MTyp.REQ:
                heapq.heappush(self.procqu, (self.lclock, m.sender))
                self.send(m.sender, typ=MTyp.ACK)
            elif m.typ == MTyp.REL:
                self.queue_remove_entry_for(m.sender)

            if m.typ in (MTyp.REQ, MTyp.ACK, MTyp.REL):
                self.debug("QUEUE:", heapq.nsmallest(len(self.procqu), self.procqu), 
                    "LAST_TS:", self.last_clocks, lvl=3)

            if self.status==St.WAIT and self.lamport_can_enter():
                self.debug("----CAN ENTER----", lvl=2)
                if self.crit_lock.locked():
                    self.crit_lock.release()
            return m

    def lamport_post(self):
        for i in range(self.size):
            self.send(i, typ=MTyp.REL)

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

    def reporting_job(self):
        while 1:
            debug("---------COLLECT---------", lvl=0)
            val = 0
            for i in range(0, self.size):
                self.send(i, typ=MTyp.REP)
                debug("SENT TO", i, lvl=0)
            resp = 0
            while resp < self.size:
                self.sem.acquire()
                m = self._recv()
                debug("RECV FR", m.sender, lvl=0)
                if m.typ == MTyp.REP:
                    val += m.data["units"]
                    resp += 1
            debug("TOTAL UNITS:", val, lvl=0)
            time.sleep(1)


    def run(self):
        CS_FUNCSET = {
            CSAlg.LAMPORT: (self.lamport_recv, self.lamport_try, self.lamport_post),
            CSAlg.RIC_AGR: (self.ricagr_recv, self.ricagr_try, self.ricagr_post)
        }
        if self.sync:
            self.sync.acquire()

        self.debug("START", lvl=2)

        if self.is_reporter:
            self.reporting_job()
        else:
            recv_f, try_f, post_f = CS_FUNCSET[self.crit_sec_alg]
            
            com_th = Thread(target=self.comm_thread, daemon=True, args=[recv_f])
            com_th.start()
            while 1:
                tid = randintnot(0, self.size-1, self.id)
                self.send_units(tid, random.randint(1, 5))
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
        self.qu: list[TMsg] = [deque() for _ in range(count+1)]
        self.sem: list[Semaphore] = [Semaphore(value=0) for _ in range(count+1)]
        self.all_th = []
        self.sync = Semaphore(value=0) if has_sync else None
    
    def spawn_threads(self, crit_sec_alg=CSAlg.LAMPORT):
        self.all_th = []
        for id in range(self.count):
            t = Worker(id, self.count, self, self.sync, crit_sec_alg)
            t.start()
            self.all_th.append(t)
        Worker(self.count, self.count, self, self.sync, is_reporter=True).start()

    def status_report(self, comp=None):
        if not comp:
            return " ".join([str(i) + ": {}".format(t.status.name) for i, t in enumerate(self.all_th[:self.count])])
        else:
            return " ".join([(str(i) if t.status==comp else "_") for i, t in enumerate(self.all_th[:self.count])])

    def start_pool(self, quiet=False):
        self.sync.release(self.count+1)
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

    DEBUG_LVL = 0
    pool.spawn_threads(crit_sec_alg=CSAlg.RIC_AGR)
    
    # pool.start_monitor()
    pool.start_pool()

    pool.join()
