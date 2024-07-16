SNAPResonseHandler Class Documentation
==========================================

.. module:: SNAPResonseHandler
   :synopsis: A class for handling warning and error messages

Overview
--------

The `SNAPResonseHandler` class is for handling :term:`SNAPResponses <SNAPResponse>`, more specifically, error handling. The main purpose is to create popup windows with relevant info to the user when an error occurs.
The handler can create a warning popup that the user can continue, or a popup that is not recoverable. In practice, a SNAPResponse is passed to the SNAPResponseHandler whenever a :term:`recipe <Recipe>` is executed.
The response contains an error code that the handler reads, and depending on the code the handler class will create an error window, a warning window, or neither.
If an error window is created, the user must retry the current step of the workflow. If a warning window is created, the user has the option to continue on to the next step.
Using signals, the handler will emit a signal to continue onto the next step if the user selects to continue anyway.

The handler also checks if the state has been initialized. If a recoverable error is thrown with "state" in the error message,
a window pops up to let the user initialize the state.

Error Codes
===========

The error codes are defined as an enum ResponseCode inside of SNAPResponse. Below are the currently defined codes.

OK
---

Value: 200
Purpose: SNAPRequest finishes with no errors to report

MAX_OK
------

Value: 300
Purpose: max value that SNAPRequest can finish with no errors to report

CONTINUE_WARNING
----------------

Value: 301
Purpose: SNAPRequest finished with less than ideal outcome, but user is allowed to continue

RECOVERABLE
-----------

Value: 400
Purpose: A recoverable exception is raised during execution of a SNAPRequest

ERROR
-----

Value: 500
Purpose: An exception is thrown during execution of a SNAPRequest
