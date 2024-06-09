import re
from collections.abc import Mapping, Sequence
from enum import Enum
from numbers import Number
from typing import Any, Dict

"""
    Construct a Nexus-compatible HDF5 representation from the dict corresponding to
    any Pydantic BaseModel instance, or reconstruct such a dict from its HDF5 representation:

    * The HDF5 encoding is contained within a single HDF5-group, marked as a Nexus `NXcollection`.
    In normal Nexus usage, this representation will be ignored by Nexus validators;

    * This allows existing methods to be used to save and restore workspaces to and from the same HDF5 file;
"""


class NexusHDF5Metadata:
    @staticmethod
    def _convert_to_scalar(s):
        if isinstance(s, Enum):
            # order matters: may be strenum or intenum
            return s.value
        elif issubclass(type(s), str) and not type(s) == str:  # noqa: E721
            # (e.g. `WorkspaceName` type): coerce it back to an _actual_ string
            return super(type(s), s).__str__()
        elif isinstance(s, (Number, str, bytes)):
            return s
        return str(s)

    @staticmethod
    def _convert_from_scalar(s):
        # recoding of <enum> is done elsewhere
        if s == "None":
            s = None
        return s

    @staticmethod
    def _traversal(node, pre=None, convert_scalar=_convert_to_scalar):
        """
        Deconstruct a dict (or an HDF5-group) into a list of traversal paths:

          * input: 'node' is any primative type, or stringifiable type,
          or any type supporting the Mapping or Sequence interfaces.

          * yield (as a generator): a single traversal path,
          which is a list of key tokens followed by the leaf-node value.
        """
        pre = list(pre) if pre else []
        if isinstance(node, (Number, Enum, str, bytes)):
            # leaf node
            yield pre + [convert_scalar(node)]
        elif not isinstance(node, (Mapping, Sequence)):
            # leaf node of _opaque_ type: will be stringified
            yield pre + [convert_scalar(node)]
        else:
            gexp = None
            if isinstance(node, Mapping):
                gexp = node.items()
            elif isinstance(node, Sequence):
                # Map a sequence to an hdf5 group structure:
                #   * encode the <sequence index> as a key: f"_{n}"
                gexp = (("_" + str(n), v) for n, v in enumerate(node))
            for key, value in gexp:
                # stringify all keys
                if isinstance(key, Enum):
                    key = key.value
                for path in __class__._traversal(value, pre + [str(key)], convert_scalar):
                    yield path

    @staticmethod
    def _reconstruct_single_path(path, cref=None, convert_scalar=_convert_from_scalar):
        # Add a single traversal path to a partially-reconstructed dict.
        def is_index(token):
            return re.match(r"_[0-9]+", token)

        def is_key(token):
            return not is_index(token)

        token = path.pop(0)
        if not path:
            # leaf node
            return convert_scalar(token)

        if is_key(token):
            if cref is None:
                cref = {}
            if isinstance(cref, dict):
                if token not in cref:
                    # attach
                    cref[token] = __class__._reconstruct_single_path(path)
                else:
                    # descend
                    cref = cref[token]
                return cref

            elif isinstance(cref, list):
                raise RuntimeError(f"key token '{token}' cannot be used as a list index")
            else:
                raise RuntimeError(f"unknown branch type (from dict): {type(cref)}")

        elif is_index(token):
            index = int(token[1:])
            if cref is None:
                cref = []
            if isinstance(cref, list):
                if index > len(cref):
                    # if required, fill-in the list with placeholders:
                    #   do not depend on traversal-paths order
                    cref.extend([None] * (index - len(cref)))
                if index == len(cref):
                    # attach
                    cref.insert(index, __class__._reconstruct_single_path(path))
                elif cref[index] is None:
                    # attach
                    cref[index] = __class__._reconstruct_single_path(path)
                else:
                    # descend
                    cref = cref[index]
                return cref

            elif isinstance(cref, dict):
                raise RuntimeError(f"index token '{token}' cannot be used as a dict key")
            else:
                raise RuntimeError(f"unknown branch type (from list): {type(cref)}")

    @staticmethod
    def _reconstruct(paths) -> Dict[str, Any]:
        """
        Construct a dict from a list of traversal paths:

          * input: 'paths' is a sequence of traversal paths, constructed using `traversal`;

          * output: dict will have leaf-nodes that are either primitive scalar types,
          or will be stringifications of the original input leaf-node types.
        """
        dict_ = {}
        for path in paths:
            cref = dict_
            while path:
                cref = __class__._reconstruct_single_path(path, cref)
        return dict_

    @staticmethod
    def _reconstruct_hdf5(root, paths):
        """
        Construct an HDF5-representation from a list of traversal paths:

          * input: 'root' is an h5py.File open in any writeable mode;

          * paths: is a sequence of traversal paths, constructed using `traversal`.
        """
        for path in paths:
            try:
                # terminal dataset 'name' with 'value'
                name = path[-2]
                value = path[-1]

                # groups branch from the specified root group:
                branchPath = "/".join(path[:-2])
                branch = root.require_group(branchPath) if branchPath else root
                branch.create_dataset(name, data=value)
            except Exception as e:
                raise RuntimeError(
                    f"Unable to construct hdf5 dataset for path: [{path}]; {name}: {value}: type: {type(value)}"
                ) from e

    @staticmethod
    def insertMetadataGroup(h5, data: Dict[str, Any], groupName="/metadata"):
        """
        Insert a metadata group into an HDF5-format file:

          * input: 'h5' is an h5py.File open in any writeable mode;

          * input: 'data' dict shall have leaf-node types that are primitive scalar types,
          or that can be trivially converted to and from string, otherwise it can
          be quite general;

          * input: groupName specifies the full path to the group in the hdf5 file,
          using standard HDF5 syntax.
        """
        paths = __class__._traversal(data)
        metadata = h5.create_group(groupName)
        metadata.attrs["NX_class"] = "NXcollection"
        __class__._reconstruct_hdf5(metadata, paths)

    @staticmethod
    def extractMetadataGroup(h5, groupName="/metadata") -> Dict[str, Any]:
        """
        Extract a metadata group from an HDF5-format file:

          * this is the complimentary method to `insertMetadataGroup` and
          is intended to work from any HDF5 group encoded in that manner;

          * input: 'h5' is an h5py.File open in any readable mode;

          * input: groupName specifies the full path to the group in the hdf5 file,
          using standard HDF5 syntax;

          * output: dict will have leaf-nodes that are either primitive scalar types,
          or will be stringifications of the original input leaf-node types.
        """

        def convert_scalar(s):
            s_ = s[()]
            if isinstance(s_, bytes):
                s_ = s_.decode("utf8")
            if s_ == "None":
                s_ = None
            return s_

        metadata = h5[groupName]
        paths = __class__._traversal(metadata, convert_scalar=convert_scalar)

        dict_ = __class__._reconstruct(paths)
        return dict_
