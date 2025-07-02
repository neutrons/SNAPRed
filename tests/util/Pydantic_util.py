from typing import Any, Dict, List


def findOffendingKeyValues(
    expected_dict: Dict[str, Any],
    actual_dict: Dict[str, Any],
) -> List[str]:
    offendingValues = []
    for key in expected_dict:
        if key not in actual_dict:
            offendingValues.append((key, expected_dict[key], None))
        elif isinstance(expected_dict[key], dict) and isinstance(actual_dict[key], dict):
            subOffendingValues = findOffendingKeyValues(expected_dict[key], actual_dict[key])
            for keyValuesTuple in subOffendingValues:
                sub_key, expected_value, actual_value = keyValuesTuple
                offendingValues.append((f"{key}.{sub_key}", expected_value, actual_value))
        elif expected_dict[key] != actual_dict[key]:
            offendingValues.append((key, expected_dict[key], actual_dict[key]))
    return offendingValues


def assertEqualModel(
    expected: object,
    actual: object,
    msg: str = "Objects are not equal",
) -> None:
    """
    Asserts equality and provides a succinct diff of two Pydantic models.
    """
    if expected != actual:
        # collect all offending keys as period delimited path and values
        expected_dict = expected.model_dump()
        actual_dict = actual.model_dump()
        offendingEntries = findOffendingKeyValues(expected_dict, actual_dict)
        if offendingEntries:
            # stringify all offending values
            offendingEntries = [
                (key, str(expected_value), str(actual_value)) for key, expected_value, actual_value in offendingEntries
            ]
            longestKey = max(len(key) for key, _, _ in offendingEntries)
            longestExpectedValue = max(len(expected_value) for _, expected_value, _ in offendingEntries)
            longestActualValue = max(len(actual_value) for _, _, actual_value in offendingEntries)
            tableCap = "-" * (longestKey + longestExpectedValue + longestActualValue + 10)
            diff = "\n" + "\n".join(
                f"{tableCap}\n"
                f"| {key.ljust(longestKey)} | {expected_value.ljust(longestExpectedValue)} | "
                f"{actual_value.ljust(longestActualValue)} |"
                for key, expected_value, actual_value in offendingEntries
            )
            diff += f"\n{tableCap}\n"
            raise AssertionError(f"{msg}: {diff}")


def assertEqualModelList(
    expected: List[object],
    actual: List[object],
    msg: str = "Lists are not equal",
) -> None:
    """
    Asserts equality of two lists of Pydantic models and provides a succinct diff.
    """
    if len(expected) != len(actual):
        raise AssertionError(f"{msg}: Length mismatch, expected {len(expected)}, got {len(actual)}")
    for i, (exp, act) in enumerate(zip(expected, actual)):
        try:
            assertEqualModel(exp, act, f"Mismatch at index {i}")
        except AssertionError as e:
            raise AssertionError(f"{msg} at index {i}: {e}")
