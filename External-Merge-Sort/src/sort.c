#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

// Για την qsort
static int compareRecords(const void* a, const void* b){
    const Record* rec1 = (Record*)a;
    const Record* rec2 = (Record*)b;
    
    int compareName = strcmp(rec1->name, rec2->name);
    if (compareName != 0) return compareName;

    int compareSurname = strcmp(rec1->surname, rec2->surname);
    return compareSurname;
}

// Ελέγχουμε με βάση το όνομα πρώτα και μετά με βάση το επώνυμο
bool shouldSwap(Record* rec1,Record* rec2){
    int compareName = strcmp(rec1->name, rec2->name);
    if (compareName > 0) return true;
    else if (compareName < 0) return false;

    int compareSurname = strcmp(rec1->surname, rec2->surname);
    if (compareSurname > 0) return true;
    
    // Ίσες εγγραφές ή με βάση τα επώνυμα δεν χρειάζεται μετάθεση
    return false;
}

// Χωρίζουμε το αρχείο σε chunks με βάση έναν iterator. Με αυτόν μετά ταξινομούμε κάθε chunk.
void sort_FileInChunks(int file_desc, int numBlocksInChunk){
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, numBlocksInChunk);
    CHUNK chunk;

    while (CHUNK_GetNext(&iterator, &chunk) != -1){
        sort_Chunk(&chunk);
    }
}

// Μεταφέρουμε όλες τις εγγραφές από ένα chunk σε έναν βοηθητικό πίνακα, τον ταξινομούμε αυτόν και έπειτα ενημερώνουμε πίσω το chunk με τις νεες εγγραφές.
void sort_Chunk(CHUNK* chunk){
    Record* records = malloc(chunk->recordsInChunk * sizeof(Record));
    if (records == NULL){
        fprintf(stderr, "Error allocating memory for sorting records in chunks.\n");
        return;
    }

    // Παίρνουμε αρχικα όλες τις εγγραφές
    for (int i = 0; i < chunk->recordsInChunk; i++){
        if (CHUNK_GetIthRecordInChunk(chunk, i, &records[i]) == -1){
            fprintf(stderr, "Error getting record %d from chunk.\n", i);
            free(records);
            return;
        }
    }

    // Ταξινομούμε
    qsort(records, chunk->recordsInChunk, sizeof(Record), compareRecords);

    // Ενημερώνουμε
    for (int i = 0; i < chunk->recordsInChunk; i++){
        if (CHUNK_UpdateIthRecord(chunk, i, records[i]) == -1){
            fprintf(stderr, "Error updating record %d in chunk.\n", i);
            free(records);
            return;
        }
    }

    free(records);
}