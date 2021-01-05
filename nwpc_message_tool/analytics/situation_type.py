from enum import Enum


class TaskSituationType(Enum):
    Initial = "initial"  # initial situation
    Submit = "submit"
    Active = "active"
    Complete = "complete"  #
    Error = "error"  # There is some error.
    Unknown = "unknown"  # There is some unknown situation.

