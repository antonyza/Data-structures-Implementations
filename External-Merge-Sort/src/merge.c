#include <merge.h>
#include <stdio.h>
#include <stdbool.h>

void merge(int input_FileDesc, int chunkSize, int bWay, int output_FileDesc ){
    CHUNK_Iterator chunkIter= CHUNK_CreateIterator(input_FileDesc, chunkSize);

    CHUNK currentChunks[bWay]; // Τα chunks που επεξεργαζόμασε
    CHUNK_RecordIterator recordIters[bWay]; // Οι αντίστοιχοι Iterators για αυτά τα chunks
    Record currentRecords[bWay]; // Οι εγγραφές από κάθε chunk που συγκρίνουμε μεταξύ τους
    int activeChunks[bWay]; // 1 αν το i-osto (i < bWay) chunk έχει άλλες εγγραφές ή οχι, 0 διαφορετικά.
    int chunksCount=0; // Πόσα chunks πήραμε κάθε φορά.
   
    // Εξωτερικό while που παίρνουμε κάθε φορα bWay chunks
    while(1) {
        // Παίρνουμε τα chunks
        chunksCount=0;
        for (int i=0; i < bWay; i++){
            if(CHUNK_GetNext(&chunkIter, &currentChunks[i])==0){
                chunksCount++;
                recordIters[i]=CHUNK_CreateRecordIterator(&currentChunks[i]);

                // Παίρνουμε τις πρώτες εγγραφές τους.
                if(CHUNK_GetNextRecord(&recordIters[i],&currentRecords[i]) == 0){
                    activeChunks[i]=1;
                }else{
                    activeChunks[i]=0; // Σε περίπτωση άδειου chunk
                }
            }else{
                break;
            }
        }
        if(chunksCount==0) return; // Συνθήκη τερματισμού να συγχωνέυσαμε όλα τα chunks (να μην μείναν άλλα)

        // Εσωτερικό while για τις εγγραφές των chunk που πήραμε (συγχώνευση)
        while(1){
            // Βλέπουμε ποιό chunk έχει το ελάχιστο στοιχείο
            int minIndex=-1;
            for(int i=0; i < chunksCount; i++){
                if(activeChunks[i]){
                    if(minIndex==-1){ // Αρχικοποίηση (στο πρώτο ενεργό chunk)
                        minIndex=i;
                    }

                    // Ουσιαστικά αν currentRecords[minIndex] > currentRecords[i]
                    if(shouldSwap(&currentRecords[minIndex], &currentRecords[i])){
                        minIndex=i;
                    }
                    
                }
            }
            if(minIndex==-1) break; // Δεν υπάρχει κανένα άλλο ενεργό chunk άρα τελειώσαμε για αυτά

            if(HP_InsertEntry(output_FileDesc, currentRecords[minIndex]) < 0){ // Output
                fprintf(stderr, "Error inserting entry into output file.\n");
                return;
            }

            // Παίρνουμε την επόμενη εγγραφή απο το chunk που κάναμε output.
            // Αν δεν έχει άλλες εγγραφές τότε απενεργοποείται.
            if(CHUNK_GetNextRecord(&recordIters[minIndex],&currentRecords[minIndex]) != 0){
                activeChunks[minIndex]=0;
            }
        }
    }
}
