# import unittest.mock as mock

# with mock.patch.dict(
#     "sys.modules",
#     {
#         "mantid.api": mock.Mock(),
#         "mantid.kernel": mock.Mock(),
#         "snapred.backend.log": mock.Mock(),
#         "snapred.backend.log.logger": mock.Mock(),
#     },
# ):
#     from snapred.backend.recipe.algorithm.ExtractionAlgorithm import ExtractionAlgorithm  # noqa: E402

#     def test_execute():
#         """Test execution of ExtractionAlgorithm"""
#         extractionAlgo = ExtractionAlgorithm()
#         extractionAlgo.initialize()
#         assert extractionAlgo.execute()
