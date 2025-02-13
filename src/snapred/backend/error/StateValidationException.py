from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class StateValidationException(Exception):
    "Raised when an Instrument State is invalid"

    """
    Implementation note:
        The previous implementation of this class actually checked the write permissions for the filename
    from the exception's stack trace.  WHY would any end user be interested in whether or not they could write
    to the file of the <python filename> where the exception was raised?!
        In changing this, I realize we _could_ meaningfully check write permissions to
    `Config['instrument.calibration.powder.home']`.  However, the problem is that a `StateValidationException`
    is more general than just this diffraction-calibration or normalization-calibration workflow case.
    For this reason I ended up only checking the exception types.  If nothing else, this gives us an interim solution.
    Further, I'll note that the permissions are now checked during the verification step at the start of each workflow,
    and if there are issues, a messagebox pops up indicating that.  For this reason, any check here, or any associated
    exception message should be redundant.
    """

    def __init__(self, exception: Exception):
        exceptionStr = str(exception)

        if isinstance(exception, (FileNotFoundError, PermissionError)):
            self.message = f"The following error occurred: {exceptionStr}\n\n" + "Please contact your IS or CIS."
        else:
            self.message = "Instrument State for given Run Number is invalid! (See logs for details.)"

        logger.error(exceptionStr)
        super().__init__(self.message)
