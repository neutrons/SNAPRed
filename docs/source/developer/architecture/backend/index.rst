Backend
=======


.. _summary:

Summary
-------

The backend is split into 3 layers, with a total of 4 conceptual components.
The layers are organized to fulfill the different levels of abstractive needs a developer requires to achieve user requirments.
The layers are as follows:


    * **Interface Layer**
        This layer is the top most layer, acting as the interface for a given frontend implementation.
        It is responsible for handling the frontend's requests and passing them to the appropriate backend component.
        It is also responsible for handling the backend's responses and passing them back to the frontend (including errors).
        The interface layer is the only layer that should be exposed to the frontend.
    * **Orchestration Layer**
        * **Service Component**
            This :term:`Component` is the data and calculation orchestration Component.
            It is responsible for stitching together calls between the :term:`Data Component` and the :term:`Recipe Component` in order to fulfill complex requirements.
            This includes things like handling a :term:`User Request`, :term:`Data State Management`, and :term:`Software Metadata`.
    * **Processing Layer**
        * **Data Component**
            This Component is the persistence Component.
            It is responsible for handling the storage and retrieval of data from the disk or wherever the data is stored.
            It provides an interface agnostic of the storage medium, allowing the :term:`Service Component` to be agnostic of the storage medium.
            It does no calculations, it just stores, retrieves, and packages data.
        * **Recipe Component**
            This Component is the calculation Component.
            It is responsible for handling the calculations required to fulfill a given :term:`User Request`.
            It provides an interface agnostic of the calculation medium, allowing the :term:`Service Component` to be agnostic of the calculation medium.
            It does no persistence, it just calculates and packages data.


Layer interaction
-----------------

The layers operate off a principle of limited visibility.

 .. mermaid::

    flowchart TD
        interface["Interface Layer"] --> orchestration["Orchestration Layer"]
        orchestration --> processing["Processing Layer"]

        subgraph processing["Processing Layer"]
        Data["Data Component"] --> Data
        Recipe["Recipe Component"] --> Recipe
        end

A lower layer is ignorant of the existence of the higher layers.
This means that the :term:`Data Component` and :term:`Recipe Component` are ignorant of the existence of the :term:`Interface Layer` and :term:`Orchestration Layer`.
The further down the stack you go, the more concrete the objectives become.

For a simplified breakdown of where you may want to implement parts of your code, refer to the :doc:`Implementation Decision Tree <../../implFlowchart>`.

.. toctree::
   :maxdepth: 1
   :caption: Index

   recipe
