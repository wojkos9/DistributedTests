from threading import Thread, Lock, Semaphore
import random
import time

from utils import *
from worker import GenericWorker
from pool import WorkerPool


class WorkerX(GenericWorker):
    def __init__(self, *args, **kwargs):
        GenericWorker.__init__(self, PTyp.X, *args, **kwargs)
        self.opp = PTyp.Y

    def process_msg(self, m: TMsg):
        r = self.standard_pairing(m)
        if not r:
            if m.typ == MTyp.END:
                if self.state > ST.WAIT:
                    self.sem.release()
            elif m.typ == MTyp.REQ:
                if m.styp == self.typ:
                    self.handle_req(m)
            elif m.typ == MTyp.ACK:
                self.ack_count += 1
                self.try_enter()
            elif m.typ == MTyp.INC:
                self.energy += 1
                if self.state == ST.PAIR:
                    self.send_req_if_ok(PTyp.X)#abovestate=ST.WAIT)
                else:
                    self.reproc_left()
    
    def work(self):
        while 1:
            self.pair = -1
            self.state = ST.WAIT
            # self.send_to_typ(PTyp.Y, MTyp.PAR)
            self.try_pair()
            self.sem.acquire()
            self.state = ST.PAIR
            self.log("---PAIR", self.pdesc(self.pair), lvl=10)

            r = self.send_req_if_ok(self.typ)

            self.sem.acquire()
            self.state = ST.CRIT
            self.send(self.pair, MTyp.STA)
            self.last_crit += 1

            time.sleep(1+random.random())
            
            self.send(self.pair, MTyp.END)
            self.sem.acquire()
            self.state = ST.IDLE
            self.release_if_ok()
            self.dec()
            # time.sleep(1)
    
class WorkerY(GenericWorker):
    def __init__(self, *args, **kwargs):
        GenericWorker.__init__(self, PTyp.Y, *args, **kwargs)
        self.opp = PTyp.X

    def process_msg(self, m: TMsg):
        if m.typ == MTyp.ACC:
            if self.state == ST.WAIT:
                self.send(m.sender, MTyp.ACC)
                self.pair = m.sender
                self.state = ST.PAIR
                self.sem.release()
        elif m.typ == MTyp.END:
            if self.state > ST.WAIT:
                self.sem.release()
        elif m.typ == MTyp.STA:
            self.sem.release()

        # elif m.typ == MTyp.PAR:
        #     if self.state == ST.WAIT:
        #         self.send(m.sender, MTyp.PAR)

    def work(self):
        # GenericWorker.work(self)
        while 1:
            self.pair = -1
            self.state = ST.WAIT
            self.send_to_typ(PTyp.X, MTyp.PAR)

            self.sem.acquire()
            self.state = ST.PAIR
            self.log("---PAIR", self.pdesc(self.pair), lvl=10)
            
            self.send_to_typ(self.opp, MTyp.FIN)
            self.state = ST.DOOR
            
            self.sem.acquire()
            self.state = ST.CRIT
            self.last_crit += 1

            time.sleep(1+random.random())
            
            self.send(self.pair, MTyp.END)
            self.sem.acquire()
            self.state = ST.IDLE
            time.sleep(1)

class WorkerZ(GenericWorker):
    def __init__(self, *args, **kwargs):
        GenericWorker.__init__(self, PTyp.Z, *args, **kwargs)

    def process_msg(self, m: TMsg):
        if m.typ == MTyp.WAK:
            if self.state == ST.IDLE:
                self.state = ST.CRIT
                self.sem.release()

    def work(self):
        while 1:
            self.sem.acquire()
            self.state = ST.CRIT
            time.sleep(1+random.random())

            self.log("INC+++++++", lvl=0)
            self.send_to_typ([PTyp.X], MTyp.INC)
            self.state = ST.IDLE
            self.pool.energy += 1
            

if __name__=="__main__":
    dbg = Debug(20)
    pool = WorkerPool((2, 2, 2), dbg,
                        classmap={PTyp.X: WorkerX, PTyp.Y: WorkerY, PTyp.Z: WorkerZ})

    pool.start()
    pool.rep_th()
    time.sleep(0.5)
    pool.join()