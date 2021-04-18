from threading import Thread, Semaphore, Lock
from collections import deque
from ds_types import *
from yoda import Worker

class WorkerPool:
    def __init__(self, cx, cy, cz, has_sync=True):
        self.counts = (cx, cy, cz)
        self.ptypes = (PTyp.X, PTyp.Y, PTyp.Z)
        self.cx, self.cy, self.cz = self.counts
        count = cx + cy + cz
        self.count = count
        self.qu: list[TMsg] = [deque() for _ in range(count)]
        self.sem: list[Semaphore] = [Semaphore(value=0) for _ in range(count)]
        self.all_th = []
        self.sync = Semaphore(value=0) if has_sync else None
        self.th_by_typ = {}
    
    def id_to_ingroup(self, id):
        idg = id
        for ci, typ in zip(self.counts, self.ptypes):
            if idg < ci:
                return idg, typ
            else:
                idg -= ci
        return -1, None
    
    def get_desc(self, id):
        return self.all_th[id].desc

    def spawn_threads(self):
        self.all_th = []
        id = 0
        for ci, typ in zip(self.counts, self.ptypes):
            self.th_by_typ[typ] = []
            for i in range(ci):
                t = Worker(id, self.count, self, typ, self.sync)
                t.start()
                self.all_th.append(t)
                self.th_by_typ[typ].append(id)
                id += 1

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