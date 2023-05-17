from snapred.backend.recipe.algorithm.ExtractionAlgorithm import ExtractionAlgorithm  # noqa: E402


def test_execute():
    """Test execution of ExtractionAlgorithm"""
    extractionAlgo = ExtractionAlgorithm()
    extractionAlgo.initialize()
    assert extractionAlgo.execute()
