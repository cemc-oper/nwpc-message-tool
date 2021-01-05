from enum import Enum

class StatusChangeType(Enum):
    Unknown = "unknown"
    Submit = "submit"
    Initial = "init"
    Complete = "complete"
    Abort = "abort"