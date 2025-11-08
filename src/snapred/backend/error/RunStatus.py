from enum import StrEnum

import mantid.api


class RunStatus(StrEnum):
    STOPPED = 'STOPPED'
    PAUSED = 'PAUSED'
    RUNNING = 'RUNNING'
    ERROR = 'ERROR'

    @classmethod
    def from_run(cls, run: mantid.api.Run) -> "RunStatus":
        """Factory method to determine the RunStatus from a Mantid Run object."""
        
        # Be careful here, this is SNAP specific:
        #   it's important to use logs which are available at SNS (e.g. no `icp_event` logs will be present).
        
        # Helper function to safely get the last value of a TimeSeriesProperty
        def get_last_value(log_name):
            if run.hasProperty(log_name):
                prop = run.getProperty(log_name)
                if hasattr(prop, "value") and len(prop.value) > 0:
                    # Extract either a single string, or the last point in a time-series.
                    if isinstance(prop.value, str):
                        return prop.value
                    return prop.value[-1]
            return None
        
        # 1. Check for an ERROR/ABORT state
        # Look at the scan abort PVs. If they evaluate to True/1, the run was aborted.
        abort_state = get_last_value("BL3:Exp:ScanAbort")
        if abort_state is None:
            abort_state = get_last_value("BL3:Exp:IM:ScanAbort")
            
        if abort_state:  # Evaluates to True if 1 or True
            return cls.ERROR

        # 2. Check if the run is officially STOPPED
        # Mantid still injects 'end_time' when the run finishes and is packaged.
        if run.hasProperty("end_time"):
            return cls.STOPPED

        # 3. Check for a PAUSED state using the dedicated 'pause' log
        pause_state = get_last_value("pause")
        if pause_state:  # Evaluates to True if 1 or True
            return cls.PAUSED
            
        # 4. Check the explicit Run Status PV (which unfortunately, usually does not exist  :(  ):
        status = get_last_value("BL3:CS:RunControl:StateEnum")
        if status is not None:
            status_str = str(status).strip().upper()
            if "PAUSE" in status_str:
                return cls.PAUSED
            elif "STOP" in status_str\
                or "IDLE" in status_str:
                return cls.STOPPED
            elif "ACQUIRING" in status_str\
                or "RECORD" in status_str\
                or "RUN" in status_str:
                return cls.RUNNING

        # 5. Fallback: If no aborts, no pauses, and no end_time, assume RUNNING
        return cls.RUNNING

# Usage:
# status = RunStatus.from_run(my_workspace.getRun())
