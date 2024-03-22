ObjectSHA Class Documentation
=============================

ObjectSHA is a Pydantic model designed to provide a standardized SHA digest for object identifiers, facilitating the implementation of
filesystem-as-database architectures. It plays a crucial role in ensuring the integrity and traceability of objects represented as JSON files within
a filesystem. The class enables verification that JSON files have not been arbitrarily relocated by incorporating the SHA digest as part of the
object's filesystem path. It also supports the verification of nested objects, where components might be loaded from separate JSON files, by equating
an object's SHA to that of its parent when necessary.


Attributes:
-----------

- hex (str): A 16-character lowercase hex string representing the SHA.

- decodedKey (Optional[str]): If the original JSON is still available, it is retained for possible re-use,
  but it is not required for the object's functionality.
