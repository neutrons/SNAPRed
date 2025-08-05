#!/usr/bin/env python3
"""
Test script to verify that the local Mantid environment is properly configured.
This script specifically checks for the issue where workbench components
might still be using site-packages instead of the local build.
"""

import os
import sys


def test_environment_setup():
    """Test that environment variables are properly set."""
    print("=== Environment Variable Check ===")
    required_vars = ["MANTID_BUILD_DIR", "MANTID_BUILD_SRC", "PYTHONPATH", "LD_LIBRARY_PATH"]

    for var in required_vars:
        value = os.environ.get(var)
        if value:
            print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NOT SET")
            return False

    return True


def test_python_path_precedence():
    """Test that local Mantid paths appear first in sys.path."""
    print("\n=== Python Path Precedence Check ===")
    mantid_build_src = os.environ.get("MANTID_BUILD_SRC")
    mantid_build_dir = os.environ.get("MANTID_BUILD_DIR")

    if not mantid_build_src or not mantid_build_dir:
        print("❌ Build paths not set")
        return False

    expected_paths = [f"{mantid_build_src}/Framework/PythonInterface", f"{mantid_build_dir}/bin"]

    print("First 5 paths in sys.path:")
    for i, path in enumerate(sys.path[:5]):
        print(f"  {i + 1}: {path}")

    # Check if expected paths are early in sys.path
    for expected in expected_paths:
        if expected in sys.path[:10]:
            print(f"✅ Found {expected} in first 10 paths")
        else:
            print(f"❌ {expected} not found in first 10 paths")
            return False

    return True


def test_mantid_import():
    """Test that Mantid imports from the correct location."""
    print("\n=== Mantid Import Test ===")
    mantid_build_src = os.environ.get("MANTID_BUILD_SRC")
    mantid_build_dir = os.environ.get("MANTID_BUILD_DIR")

    try:
        import mantid

        mantid_location = mantid.__file__
        print(f"Mantid imported from: {mantid_location}")

        # Check if it's coming from our local build
        if mantid_build_src and mantid_build_src in mantid_location:
            print("✅ Mantid imported from local SOURCE directory")
            return True
        elif mantid_build_dir and mantid_build_dir in mantid_location:
            print("✅ Mantid imported from local BUILD directory")
            return True
        else:
            print("❌ Mantid imported from conda/site-packages (not local build)")
            return False

    except ImportError as e:
        print(f"❌ Failed to import Mantid: {e}")
        return False


def test_workbench_components():
    """Test that workbench components use the local build."""
    print("\n=== Workbench Components Test ===")
    mantid_build_src = os.environ.get("MANTID_BUILD_SRC")
    mantid_build_dir = os.environ.get("MANTID_BUILD_DIR")

    components = ["mantidqt", "mantidqt.utils.qt"]
    results = []

    for component in components:
        try:
            if component == "mantidqt.utils.qt":
                # This doesn't have a __file__ attribute, so we check mantidqt
                import mantidqt
                from mantidqt.utils.qt import import_qt  # noqa: F401

                location = mantidqt.__file__
            else:
                module = __import__(component)
                location = module.__file__

            print(f"{component} imported from: {location}")

            # Check if it's from local build
            if mantid_build_src and mantid_build_src in location:
                print(f"✅ {component} using local SOURCE")
                results.append(True)
            elif mantid_build_dir and mantid_build_dir in location:
                print(f"✅ {component} using local BUILD")
                results.append(True)
            else:
                print(f"❌ {component} using conda/site-packages")
                results.append(False)

        except ImportError as e:
            print(f"❌ Failed to import {component}: {e}")
            results.append(False)

    return all(results)


def main():
    """Run all tests."""
    print("Local Mantid Environment Test Suite")
    print("=" * 50)

    tests = [test_environment_setup, test_python_path_precedence, test_mantid_import, test_workbench_components]

    results = []
    for test in tests:
        results.append(test())

    print("\n" + "=" * 50)
    print("SUMMARY:")
    if all(results):
        print("✅ All tests passed! Local Mantid environment is properly configured.")
        return 0
    else:
        print("❌ Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
