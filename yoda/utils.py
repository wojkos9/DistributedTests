from enum import Enum
from dataclasses import dataclass
import random
from threading import Lock

class PTyp(Enum):
    X = 0
    Y = 1
    Z = 2
    NONE = 3

class MTyp(Enum):
    REQ = 0
    ACK = 1
    PAR = 2
    ACC = 3
    STA = 4
    END = 5
    FIN = 6
    WAK = 7
    INC = 8
    TER = 99

class ST(Enum):
    IDLE = 0
    WAIT = 1
    PAIR = 2
    DOOR = 3
    CRIT = 4
    def __lt__(self, s2):
        return self.value < s2.value

@dataclass
class TMsg:
    typ: MTyp
    sender: int
    styp: PTyp
    data: dict
    cl: int

class Debug:
    def __init__(self, DEBUG_LVL):
        self.DEBUG_LVL = DEBUG_LVL
        random.seed(1)
        self.waits = [random.random() for _ in range(100)]
        self.lock = Lock()

    def log(self, *args, **kwargs):
        if "lvl" in kwargs:
            lvl = kwargs["lvl"]
            del kwargs["lvl"]
        else:
            lvl = 10
        if lvl <= self.DEBUG_LVL:
            print(*args, **kwargs)

    def rand(self, pid, size):
        self.lock.acquire()
        i = next(iter([i for i, t in enumerate(self.waits) if i%size==pid and t != None]))
        t = self.waits[i]
        self.waits[i] = None
        self.lock.release()
        return t

DEBUG10 = Debug(10)