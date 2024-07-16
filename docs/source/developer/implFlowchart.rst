Implementation Decision Tree
============================

 .. mermaid::

    flowchart TD
        id[Start] --> id1["Does it Load/Save Data from/to Disk?"]
        id1 --> |Yes| id2["Does it Transform Data?"]
        id1 --> |No| id3["Does it Calculate Data?"]
        id2 --> |Yes| id4["Implement in the Service Layer"]
        id4 --> id9["Reconsult Decision Tree for Disk and Transform Components"]
        id9 --> id
        id2 --> |No| id5["Implement in the Data Layer"]
        id3 --> |Yes| id6["Implement in the Recipe Layer"]
        id3 --> |No| id7["Is it software metadata?"]
        id7 --> |Yes| id4
        id7 --> |No| id8["Does it actually do anything?"]


..
    I'm not sure if linking to existing docs is relavent within this flowchart but I believe to
    have found the proper way of adding tags to mermaid produced diagrams. Here are two examples
    for different types of tagging:

    1. Hyperlinks -

        id[Start] --> id1["Does it <a href="https://www.exampleofLoad.com">Load</a>/Save Data from/to Disk?"]

      In this example above, the link will be applied to the text "Load" within the diagram.

    2. Doc Tags -

        id[Start] --> id1["Does it <a href=":doc:`exampleofLoad`">Load</a>/Save Data from/to Disk?"]

      Similar to the example for hyperlinks, the doc link above will be applied to the text "Load" within the diagram.
