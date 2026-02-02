# Database Systems Implementation
A low-level implementation of core database systems (Heap Files, BPlus Trees and Memory Chunk Management)

Written in C with a provided block-level simulation.

Developed as part of the **Implementation of Database Systems** (Υλοποίηση Συστημάτων Βάσεων Δεδομένων) coursework at **National and Kapodistrian University of Athens** (Εθνικό και Καποδιστριακό Πανεπιστήμιο Αθηνών)

## Features
- Heap File: Managing records within fixed-size memory blocks.
- BPlus Tree: Efficient search, insertion and deletion of nodes, handling complex node splits.
- External Merge Sort: Efficient algorithm for sorting large datasets that do not fit in RAM.

## Provided Resources
- Low-level Block File Handler (libbf.so)
- Record Data Structure (record.c)
- Heap File Handler (for External Merge Sort only)

## My contribution
- [Heap File Implementation](./Heap-File)
- [BPlus Tree Implementation](./BPlus-Tree)
- [External Sort](./External-Merge-Sort)
