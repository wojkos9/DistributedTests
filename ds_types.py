from enum import Enum
from dataclasses import dataclass

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

class PTyp(Enum):
    X=1
    Y=2
    Z=3