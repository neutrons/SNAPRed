Normalization Class Documentation
=================================

Normalization.py introduces the Normalization class, aimed at facilitating the process of data normalization by incorporating contextual details such
as the instrument's current state (instrumentState), the identifier of the original run (seedRun), and metadata including the creation date
(creationDate), name (name), and version (version).

Upon initialization, the class captures all necessary attributes to provide a comprehensive normalization context. Its primary function is to enable
precise normalization, taking into account the instrument's current state, and supporting traceability (seedRun), temporal tracking (creationDate),
and iterative improvements (version). This method enhances normalization execution, data integrity, and analytical results' effectiveness.


Attributes:
-----------

- instrumentState (InstrumentState): Represents the current state of the instrument. This
  is critical for ensuring that the normalization is appropriate for the instrument's
  conditions at the time of the run.

- seedRun (int): Identifier for the initial run from which this normalization object is
  generated. It is essential for traceability and reproducibility of the normalization process.

- creationDate (datetime): Records the exact date and time when this normalization object was
  created, providing a timestamp for versioning and historical reference.

- name (str): A descriptive name given to this normalization object for easy identification
  and reference within a dataset or system.

- version (int, default=0): Version number of this normalization object, allowing for
  version control and updates to normalization parameters or methods over time.
