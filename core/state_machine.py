import enum

class PipelineStatus(enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"

class StateMachine:
    """
    Centralized logic to determine if a state transition is valid,
    or to determine the aggregate state of a study based on its components.
    """
    
    @staticmethod
    def is_valid_transition(current_state: PipelineStatus, new_state: PipelineStatus) -> bool:
        """
        Define valid state transitions here to prevent race conditions from 
        overwriting a terminal state with an earlier state.
        """
        if current_state == new_state:
            return True
            
        if current_state == PipelineStatus.COMPLETED:
            # Cannot move backwards from completed, unless it's a forced retry/failed.
            if new_state in (PipelineStatus.FAILED, PipelineStatus.PENDING):
                return True
            return False
            
        if current_state == PipelineStatus.FAILED:
            # From failed, we can only go to pending (retry)
            if new_state == PipelineStatus.PENDING:
                return True
            return False
            
        # Pending and Processing can transition to anything.
        return True
