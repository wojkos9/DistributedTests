from threading import Thread, Semaphore, Lock
from collections import deque
import heapq
import time
import random
import sys

from utils import queue_remove_entry_for

from ds_types import *

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

class GenericWorker(Thread):
    def __init__(self, id, size, pool, ptype, sync=None):
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
        self.ptype = ptype
        self.cx, self.cy, self.cz = pool.counts
        i, t = pool.id_to_ingroup(self.id)
        self.desc = f"{t.name.lower()}{i}"
        self.pqus = {PTyp.X: [], PTyp.Y: [], PTyp.Z: []}
        self.pair = None

    def debug(self, *args, **kwargs):
        debug("({}) [{}]".format(self.desc, self.id), *args, **kwargs)

    def debug_long(self, TAG, *args, **kwargs):
        c="-"
        if type(TAG)==tuple and len(TAG)==2:
            TAG, c = TAG
        lens=(10, 8, 10)
        pad = " " * max(0, lens[1]-len(TAG)-len(self.desc))
        msg = "-"*lens[0]+TAG+pad+self.desc+"-"*lens[2]+c
        debug(msg, *args, **kwargs)

    def send(self, tid, typ=MTyp.DEF, data={}, exception=None, verb=False):
        if type(exception)==int and exception==tid or type(exception)==list and tid in exception:
            return
        else:
            self.lclock += 1
            self.pool.qu[tid].append(TMsg(typ, self.id, data, self.lclock))
            self.pool.sem[tid].release()
            if verb:
                print(f"{self.desc} SEND TO {self.desc_for(tid)}")
    
    def send_to_typ(self, ptyp:PTyp, **kwargs):
        for i in self.pool.th_by_typ[ptyp]:
            self.send(i, exception=self.id, **kwargs)

    def get_typ(self, id):
        return self.pool.id_to_ingroup(id)[1]

    def desc_for(self, id):
        return self.pool.get_desc(id)

    def _recv(self) -> TMsg:
        q = self.qu
        if q:
            m = q.popleft()
            lclock = max(self.lclock, m.cl) + 1

            prio = 2 if m.typ != MTyp.DEF else 3
            self.debug("[<-{}-{}({})] [cl {}->{}] RECV: cl={}, data=\"{}\""
                .format(m.typ.name, m.sender, self.pool.get_desc(m.sender), self.lclock, lclock, m.cl, m.data), lvl=prio)
            
            self.lclock = lclock
            return m
        else:
            return None

    def critical_section(self, sect_time=4):
        global all_th
        self.status = St.CRIT
        self.debug_long(("CRIT", "<"))

        if self.ptype == PTyp.X:
            # for p in self.pqus[PTyp.Y]:
            #     self.send(p, MTyp.ACK, verb=True)
            self.send_to_typ(PTyp.Y, typ=MTyp.ACK, verb=True)
            self.crit_lock.acquire()

        for i in range(sect_time):
            self.debug("[msg {}/{}] IN CRIT:".format(i+1, sect_time), self.pool.status_report(St.CRIT),
                "WAIT:", self.pool.status_report(St.WAIT), lvl=1)
            time.sleep(1)
        
        self.debug_long(("LEAVE", ">"))
        self.status = St.IDLE

    def pqu(self, m):
        return self.pqus[self.get_typ(m.sender)]

    def ricagr_try(self):
        if self.status == St.IDLE:
            self.debug_long(("TRY", "?"))
            self.status = St.WAIT
            self.ricagr_req = self.lclock
            self.ricagr_count = 0

            data = {"prio": self.ricagr_req}
            args = {"typ": MTyp.REQ, "data": data}

            if self.ptype == PTyp.X:
                self.send_to_typ(PTyp.X, **args)
                self.send_to_typ(PTyp.Z, **args)
            elif self.ptype == PTyp.Y:
                self.send_to_typ(PTyp.Y, **args)
                self.send_to_typ(PTyp.X, **args)
            elif self.ptype == PTyp.Z:
                # self.send_to_typ(PTyp.X, **args)
                # self.send_to_typ(PTyp.Z, **args)
                pass
            
            return True
        return False
    
    def ricagr_recv(self):
        m = self._recv()

        styp = self.get_typ(m.sender)
        resp_all = False
        if self.ptype == PTyp.X:
            need_size = self.cx + self.cz - 1
            resp_all = styp==PTyp.Y and self.status == St.CRIT
        elif self.ptype == PTyp.Y:
            need_size = self.cy
        elif self.ptype == PTyp.Z:
            need_size = self.cx
            resp_all = True

        if m:
            if m.typ == MTyp.REQ:
                prio = m.data["prio"]
                # if self.get_typ(m.sender) != self.ptype:
                #     self.send(m.sender, typ=MTyp.ACK)
                if styp == self.ptype:
                    if self.status != St.IDLE and (self.ricagr_req, self.id) < (prio, m.sender):
                        heapq.heappush(self.procqu, (prio, m.sender))
                        heapq.heappush(self.pqu(m), (prio, m.sender))
                    else:
                        self.send(m.sender, typ=MTyp.ACK, verb=resp_all)
                elif resp_all:
                    self.send(m.sender, typ=MTyp.ACK, verb=resp_all)
                
            elif m.typ == MTyp.ACK:
                if self.status == St.WAIT:
                    self.ricagr_count += 1
                    if self.ptype == PTyp.Y and styp == PTyp.X:
                        self.pair = m.sender
                    if self.ricagr_count == need_size:
                        if self.ptype == PTyp.Y:
                            if self.pair is None:
                                raise Exception("Y NO PAIR")
                            else:
                                self.send(self.pair, MTyp.PAR)
                                self.debug_long(f"YPAR {self.desc_for(self.pair)}")
                        if self.crit_lock.locked():
                            self.crit_lock.release()
            elif m.typ == MTyp.PAR and self.ptype == PTyp.X:
                self.pair = m.sender
                self.debug_long(f"XPAR {self.desc_for(self.pair)}")
                if self.crit_lock.locked():
                    self.crit_lock.release()

    def ricagr_post(self):
        self.pair = None
        if self.ptype != PTyp.Y:
            for _, tid in heapq.nsmallest(len(self.procqu), self.procqu):
                self.send(tid, typ=MTyp.ACK)
        self.ricagr_count = 0
        self.procqu = []
        for k in self.pqus.keys():
            self.pqus[k] = []
    
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
                    self.debug_long(("WAIT", "|"))
                    self.crit_lock.acquire()
                    self.critical_section()
                    if post_f:
                        post_f()
            time.sleep(2*random.random())
        self.debug("FIN", lvl=2)

DEBUG_LVL = 2
from worker_pool import *


if __name__=="__main__":
    pool = WorkerPool(4, 4, 4)

    
    pool.spawn_threads()
    
    # pool.start_monitor()
    pool.start_pool()

    pool.join()
