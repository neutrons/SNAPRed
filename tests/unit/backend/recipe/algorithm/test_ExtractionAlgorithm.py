import unittest.mock as mock

with mock.patch.dict("sys.modules", {"mantid.api": mock.Mock()}):
    from snapred.backend.recipe.algorithm.ExtractionAlgorithm import ExtractionAlgorithm  # noqa: E402

    def test_execute():
        """Test exection of ExtractionAlgorithm"""
        extractionAlgo = ExtractionAlgorithm()
        extractionAlgo.execute()

        print(extractionAlgo.log().notice.call_args)
        assert extractionAlgo is not None
        assert extractionAlgo.log.called
        assert extractionAlgo.ExtractionIngredients is not None
