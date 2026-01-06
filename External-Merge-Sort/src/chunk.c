#include <merge.h>
#include <stdio.h>
#include "chunk.h"


CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk){
    CHUNK_Iterator iterator;
    iterator.blocksInChunk = blocksInChunk;
    iterator.file_desc = fileDesc;
    iterator.current=1;
    iterator.blocksInChunk=blocksInChunk;
    iterator.lastBlockID=HP_GetIdOfLastBlock(fileDesc);
    return iterator;

}

int CHUNK_GetNext(CHUNK_Iterator *iterator,CHUNK* chunk){
    // Σε περίπτωση που δεν υπάρχει άλλο chunk
    if(iterator->current > iterator->lastBlockID){
        return -1;
    }

    // Υπολογισμος του επόμενου chunk
    chunk->file_desc = iterator->file_desc;
    chunk->from_BlockId = iterator->current;

    int endBlock = iterator->current + iterator->blocksInChunk - 1;
    if(endBlock > iterator->lastBlockID){
        endBlock = iterator->lastBlockID;
    }
    chunk->to_BlockId=endBlock;
    chunk->blocksInChunk=(endBlock - iterator->current)+1;

    // Υπολογισμος συνολικών εγγραφών στο chunk
    chunk->recordsInChunk = 0;
    for(int i = chunk->from_BlockId; i<=chunk->to_BlockId; i++){
        chunk->recordsInChunk+=HP_GetRecordCounter(iterator->file_desc, i);
    }

    // Ενημερωση του iterator για το επομενο call (chunk)
    iterator->current += iterator->blocksInChunk;
    return 0;
}

int CHUNK_GetIthRecordInChunk(CHUNK* chunk,  int i, Record* record){
    int currentBlockID = chunk->from_BlockId;
    int recordsCount = 0;

    // Διατρέχουμε όλα τα blocks του chunk
    while (currentBlockID <= chunk->to_BlockId){
        int recordsInBlock = HP_GetRecordCounter(chunk->file_desc, currentBlockID);

        // Άμα το πλήθος των εγγραφών που έχουμε βρεί μέχρι τώρα (μαζί με αυτό το block) είναι μικρότερο του i τότε η εγγραφή βρίσκεται αναγκαστικά σε αυτό το block
        if (i < recordsCount + recordsInBlock){
            int cursor = i - recordsCount; // Θέση της εγγραφής στο block auto (i < recordsInBlock)
            int result = HP_GetRecord(chunk->file_desc, currentBlockID, cursor, record);
            HP_Unpin(chunk->file_desc, currentBlockID);
            return result;
        }   

        recordsCount += recordsInBlock;
        currentBlockID++;
    }
    
    // Άμα δεν βρέθηκε μέχρι τώρα τότε σημαίνει ότι το i είναι μεγαλύτερο απο τις συνολικές εγγραφές
    return -1;
}

int CHUNK_UpdateIthRecord(CHUNK* chunk,  int i, Record record){
    // Ίδια λογική με πάνω
    int currentBlockID = chunk->from_BlockId;
    int recordsCount = 0;
    while (currentBlockID <= chunk->to_BlockId){
        int recordsInBlock = HP_GetRecordCounter(chunk->file_desc, currentBlockID);

        if (i < recordsCount + recordsInBlock){
            int cursor = i - recordsCount;
            int result = HP_UpdateRecord(chunk->file_desc, currentBlockID, cursor, record);
            HP_Unpin(chunk->file_desc, currentBlockID);
            return result;
        }   

        recordsCount += recordsInBlock;
        currentBlockID++;
    }
    return -1;

}

void CHUNK_Print(CHUNK chunk){
    printf("--- Printing Chunk Info ---\n");
    printf("File Desc: %d\n",chunk.file_desc);
    printf("Blocks: %d to %d\n",chunk.from_BlockId, chunk.to_BlockId);
    printf("Total Records: %d\n", chunk.recordsInChunk);

    CHUNK_RecordIterator iter = CHUNK_CreateRecordIterator(&chunk);
    Record rec;
    int count = 0;
    while(CHUNK_GetNextRecord(&iter, &rec) == 0){
        printf("[%d] Record: ID=%d, Name=%s, Surname=%s, City=%s\n", count, rec.id, rec.name, rec.surname, rec.city);
        count++;
    }
    printf("--- End of chunk ---\n");
}


CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk){
    CHUNK_RecordIterator iterator;
    iterator.chunk = *chunk;
    iterator.currentBlockId = chunk->from_BlockId;
    iterator.cursor = 0;
    return iterator;
}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator, Record* record){
    // Σε περίπτωση που η επόμενη εγγραφή βρίσκεται στο επόμενο block τότε αλλάζουμε block στον record iterator
    // Εφαρμόζουμε while ώστε αν τύχει καμιά η περίπτωση άδειων block.
    while (iterator->currentBlockId <= iterator->chunk.to_BlockId){
        int recordsInBlock = HP_GetRecordCounter(iterator->chunk.file_desc, iterator->currentBlockId);

        if (iterator->cursor < recordsInBlock){ // Παραμένουμε στο ίδιο block
            int result = HP_GetRecord(iterator->chunk.file_desc, iterator->currentBlockId, iterator->cursor, record);
            HP_Unpin(iterator->chunk.file_desc, iterator->currentBlockId);

            iterator->cursor++;
            if (iterator->cursor >= recordsInBlock){ // Αλλάζουμε block άμα το ξεπεράσαμε με την ανάκτηση αυτή
                iterator->currentBlockId++;
                iterator->cursor = 0;
            }

            return result;
        }else{ // Αλλάζουμε block
            iterator->currentBlockId++;
            iterator->cursor = 0;
        }
    }
    return -1;
}
