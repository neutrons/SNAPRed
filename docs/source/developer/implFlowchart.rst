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
