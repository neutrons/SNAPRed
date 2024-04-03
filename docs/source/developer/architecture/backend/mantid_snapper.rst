Mantid Snapper
==============
The Mantid Snapper class is a wrapper around the :term:`Mantid` Algorithm API that allows for meta processes to be performed around a queue of algorithms.
Examples may include: Progress reporting, Quality of Life improvements, multi-threading, retrieving algorithm output properties, etc.


Example Usage
-------------

In a :term:`Recipe`, create a new MantidSnapper object. Then using the object, call any :term:`Mantid` algorithm and fill out the parameters.
You can search for algorithms at https://docs.mantidproject.org. You can use the mtd function to retrieve data from a :term:`Workspace`.
Lastly, call executeQueue() to run the algorithm.

Example Code
------------

.. code-block:: python

    from mantid.api import PythonAlgorithm
    from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


    class ExampleAlgorithm(PythonAlgorithm):
        def PyInit(self):
            self.mantidSnapper = MantidSnapper(self, __name__)

        def PyExec(self):
            self.mantidSnapper.SampleMantidAlgorithm1(
                "Insert log message here...",
                #   Params="",
            )

            # Use mtd to retrieve data from a workspace
            self.mantidSnapper.mtd[workspace]

            self.mantidSnapper.SampleMantidAlgorithm2(
                "Insert log message here...",
                #   Params="",
            )
            self.mantidSnapper.executeQueue()
