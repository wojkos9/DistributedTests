from utils import *
from threading import Thread, Lock, Semaphore
from collections import deque
from itertools import chain
import heapq
import random
import time
from collections.abc import Iterable

class GenericWorker(Thread):
    def __init__(self, typ, pid, pool, debug=None, ene=None):
        Thread.__init__(self, daemon=True)
        self.typ = typ
        self.pid = pid
        self.size = pool.size
        self.cx, self.cy, self.cz = pool.counts
        self.pool = pool
        self.debug: Debug = debug
        self.queue: deque = pool.queues[pid]
        self.msem = pool.msems[pid]
        self.state = ST.IDLE
        self.work_th = Thread(target=self.work, daemon=True)
        self.desc = "%d-%s" % (pid, typ.name)
        self.desc_long = "%d-%s-%02d" % (pid, typ.name, pool.id_in_group(pid))

        self.clock = 0
        self.pqus = {t: [] for t in pool.types}
        self.pqu = []
        self.sem = Semaphore(0)
        self.opp = PTyp.Y if self.typ==PTyp.X else (PTyp.X if self.typ==PTyp.Y else PTyp.NONE)
        self.copp, self.cown =  (self.cy, self.cx) if self.typ==PTyp.X else  (self.cx, self.cy)
        self.own_req = None
        self.ack_count = 0
        self.order = 0
        self.oppr = 0
        self.pair = -1
        self.last_crit = 0
        self.energy = ene if ene else self.cz


    def mpqu(self, m):
        return self.pqus[self.pool.typemap[m.sender]]

    def putqu(self, m, qu=None):
        if qu is None:
            qu = self.mpqu(m)
        #heapq.heappush(qu, (m.cl, m.sender))
        qu.append((m.cl, m.sender))
        qu.sort()

    def lpopqu(self, typ):
        qu = self.pqus[typ]
        if qu:
            r, qu = qu[0], qu[1:]
            return r
        return None
    
    def delqu(self, m):
        # print(self.pqus[self.opp], "-", m.sender)
        self.pqus[m.styp] = list(filter(lambda e: e[1] != m.sender, self.pqus[m.styp]))
        # print(self.pqus[self.opp])
        pass

    def log(self, *args, **kwargs):
        if self.debug:
            self.debug.log("[%4s]" % (self.desc), *args, f"@ {self.clock}", **kwargs)

    def send(self, tid, mtyp, data={}, cl=None):
        self.clock += 1
        if cl is None:
            cl = self.clock
        m = TMsg(mtyp, self.pid, self.typ, data, cl)
        self.pool.send(tid, m)
        self.log(   ">%4s" % (self.pool.threads[tid].desc), 
                    m.typ.name,
                    f"[cl={m.cl}]",
                    m.data, 
                    lvl=20)

    def send_to_typ(self, typ, mtyp, **kwargs):
        for t in typ if isinstance(typ, Iterable) else [typ]:
            # cl = self.clock+1
            for tid in self.pool.get_of_type(t):
                if tid != self.pid:
                    self.send(tid, mtyp, **kwargs)

    def pdesc(self, pid):
        return self.pool.desc(pid)

    def _recv(self):
        self.msem.acquire()
        # time.sleep(0.2+random.random()*0.3)
        m = self.queue.popleft()
        self.clock = max(self.clock, m.cl) + 1
        self.log(   "<%4s" % (self.pdesc(m.sender)), 
                    m.typ.name,
                    f"[cl={m.cl}]",
                    m.data, 
                    lvl=30)
        if m.typ == MTyp.TER:
            exit(0)
        return m
    
    def process_msg(self, m: TMsg):
        pass

    def dec(self):
        self.log("DEC", lvl=19)
        self.pool.dec()
        if self.typ == PTyp.X and self.energy == 0:
            self.log("-----ZEROZERO=======")
            self.send_to_typ(PTyp.Z, MTyp.WAK)

    def standard_pairing(self, m: TMsg):
        if m.typ == MTyp.PAR:
            if self.pair == -1:
                self.pair = m.sender
                self.send(m.sender, MTyp.ACC)
            else:
                self.log(f"QU {m.sender}", lvl=21)
                self.putqu(m)
        elif m.typ == MTyp.FIN:
            if m.styp == self.opp:
                self.delqu(m)
                self.log(f"UNQU {m.sender}", lvl=21)
                if m.sender == self.pair and self.state == ST.WAIT:
                    nxt = self.lpopqu(self.opp)
                    if nxt:
                        tid = nxt[1]
                        self.pair = tid
                        self.send(tid, MTyp.ACC)
                    else:
                        self.pair = -1
                return True
            return False
        elif m.typ == MTyp.ACC:
            self.state = ST.PAIR
            self.sem.release()
        else:
            return False
        return True

    def send_req_if_ok(self, to_typ):
        if self.energy > 0:
            self.clock = (cl := self.clock+1)
            self.own_req = (cl, self.pid)
            self.ack_count = 0
            self.energy -= 1
            self.log("SEND REQ", lvl=9)
            self.send_to_typ(to_typ, MTyp.REQ, cl=cl)
            self.state = ST.DOOR
            self.try_enter()
            return True
        return False

    def reenter(self):
        self.state = ST.DOOR
        self.energy -= 1
        self.try_enter()

    def try_enter(self, abovestate=ST.PAIR):
        if self.state == ST.DOOR and self.ack_count >= self.cown - 1 - self.energy:
            self.state = ST.CRIT
            self.sem.release()
            self.log("ENTER", lvl=9)
            return True
        self.log("CANT ENTER///////", self.state.name, lvl=0)
        return False

    def handle_req(self, m: TMsg, canqu=False):
        req = (m.cl, m.sender)
        self.log("PROC REQ", req, self.own_req, self.energy)
        should_qu = False
        if self.energy > 0:
            if self.state == ST.DOOR and self.own_req < req:
                should_qu = True
        else:
            should_qu = True

        if should_qu:
            self.putqu(m)
        else:
            self.log("RESPOND REQ", req, self.own_req, self.energy)
            self.send(m.sender, MTyp.ACK)
            self.energy -= 1
    
    def proc_left(self):
        q = self.pqus[self.typ][:]
        self.pqus[self.typ] = []
        for r in q:
            self.handle_req(TMsg(MTyp.REQ, r[1], PTyp.X, {}, r[0]))

    def release_if_ok(self):
        while self.energy > 0:
            nxt = self.lpopqu(self.typ)
            if nxt is not None:
                tid = nxt[1]
                self.energy -= 1
                self.send(tid, MTyp.ACK)
            else:
                break

    def try_pair(self):
        if self.pqus[self.opp]:
            tid = self.lpopqu(self.opp)[1]
            self.pair = tid
            self.send(tid, MTyp.ACC)
            return True
        return False

    # def work(self):
    #     to = random.randint(0, self.size-1)
    #     self.send(to, MTyp.REQ, {"pid": self.pid})
    #     time.sleep(2)
        # self.log("CRIT")
        # self.log(self.pqus[self.typ], "OWN", self.own_req)

    def run(self):
        self.work_th.start()
        while 1:
            m = self._recv()
            self.process_msg(m)