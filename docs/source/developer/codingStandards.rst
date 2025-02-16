Coding Standards
================

.. _comments:

Comments
--------

Comments should be used to provide intuition, clarify abnormal code, and/or break down the logicial flow of a method.
Comments are not a substitution for clear code, they should be an explanation of concepts that lead to the code.
Comments require maintenance, be succinct.

Comments are also useful for mapping known issues or planned refactors.

Provide Intuition
`````````````````

.. code-block:: python

    # GOOD
    # Version may be 0, and interpreted as False
    if version is None:
        # ...

    # conversion factor from microsecond/Angstrom to meters
    CONVERSION_FACTOR = Config["constants.m2cm"] * PhysicalConstants.h / PhysicalConstants.NeutronMass


    # OK - What is LSS?
    def LLSTransformation(self, input):  # noqa: A002
        # Applies LLS transformation to emphasize weaker peaks
        return np.log(np.log((input + 1) ** 0.5 + 1) + 1)

    # BAD
    # This is a loop that iterates over the histograms of a snap workspace
    for h in ws.getSpectra():
        # ...

Clarify Abnormal Code
`````````````````````

.. code-block:: python

    # GOOD - call out odd and speficic behavior
    # NOTE: This is a hack to work around a bug in the library
    LoadRawData(ws)
    ReloadInstrumentData(ws)
    ws.setImportantFlag(True)

    # BAD - Masks side effects of a function and is misleading
    def checkIfEmpty(self, data):
        # This checks if a dictionary is empty
        for k,v in data.copy().items():
            if k is "someRandomKey":
                data.delete(k)
            if v is None:
                return True

Flow of Code
````````````
Presume the following code is more complex than it is.

.. code-block:: python

    # GOOD
    # 1. Load raw data for a good comment
    # 2. Extract features for a good comment
    # 3. Compose good comment
    # 4. Display the end result.
    # (arguably, this brand of comment could just be broken up into descriptive methods.)

    # 1
    dataGetterService = DataGetterService()
    dataGetterService.initializeConnection()
    dataGetterService.handShake()
    rawData = dataGetterService.getRawData()

    # 2
    featureExtractorService = FeatureExtractorService()
    features = featureExtractorService.extractFeatures(rawData)

    # 3
    goodComment = " ".join(features)

    # 4
    print(goodComment)


    # BAD
    # The point of this section of code is to load raw data quickly and effectively for a good comment for our users.
    # but first we need to initialize a stable connection, then we need to handshake for added security,
    # then we need to get the raw data from the appropriate source.
    # Next we need to process the raw data using the feature extractor service.
    # Then we need to compose the good comment by joining the features together.
    # Finally we need to display the end result with a print statement.
    # (this is a sales pitch, not a comment)
    dataGetterService = DataGetterService()
    dataGetterService.initializeConnection()
    # this is necessary or else we can't get the raw data for some reason
    dataGetterService.handShake()
    rawData = dataGetterService.getRawData()  # gets the raw data
    featureExtractorService = FeatureExtractorService()
    features = featureExtractorService.extractFeatures(rawData)
    # not the best solution but...
    goodComment = " ".join(features)
    print(goodComment)


Standardized Comments
---------------------


Known Issues
````````````

In SNAPRed we use a standardized comment to call out sections of code that need to change.
The format is as follows:

.. code-block:: python

    # TODO: EWM#{issue_number} - {description of issue}

This covers hacks and workarounds as well as planned refactors.
If the section of code is rather long, consider wrapping it with a `- Begin` and `- End` comment.
