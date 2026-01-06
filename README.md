# 🖥️ Database Systems Implementation
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
- [x] Heap File Implementation
- [x] BPlus Tree Implementation
- [x] External Sort
- [x] Testing scenarios to verify integrity for the above

## :gear: How to Run
### Requirements
- OS: Linux or Windows via WSL
- Compiler: GCC

### Steps
1. Clone the repository:
```
git clone https://github.com/antonyzah/DBMS-Data-Structures-Implementations.git
cd DBMS-Data-Structures-Implementations
```
2. Navigate to the desired implementation folder and run the test:

Heap File:
```
cd Heap-File
make hp_run
```
BPlus Tree:
```
cd BPlus-Tree
make bplus_run
```
External Merge Sort:
```
cd External-Merge-Sort
make sort_run
```
