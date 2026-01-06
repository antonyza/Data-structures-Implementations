#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "merge.h"
#include "chunk.h"
#include "hp_file.h"
#include "sort.h"
#include "bf.h"
#include "record.h"

#define RECORDS_NUM 500 // you can change it if you want
#define FILE_NAME "data.db"
#define OUT_NAME "out"

int createAndPopulateHeapFile(char* filename);
void sortPhase(int file_desc,int chunkSize);
void mergePhases(int inputFileDesc,int chunkSize,int bWay, int* fileCounter);
int nextOutputFile(int* fileCounter);
void checkIfSorted(char * filename);


int main() {
	BF_Init(LRU);


	// 1ο παράδειγμα
	int chunkSize=5;
	int bWay= 2;
	int fileIterator = 0;
	if (system("rm -f data.db out*.db") == -1) {} // Άμα υπάρχουν ήδη
	printf("Example 1: Blocks per Chunk = %d | B-Way = %d\n", chunkSize, bWay);

	int file_desc = createAndPopulateHeapFile(FILE_NAME);
	sortPhase(file_desc,chunkSize);
	mergePhases(file_desc,chunkSize,bWay,&fileIterator);

	if (fileIterator > 0) {
		char lastfile[50];
		sprintf(lastfile, "%s%d.db", OUT_NAME,fileIterator -1);
		checkIfSorted(lastfile);
	}
	else {
		checkIfSorted(FILE_NAME);
	}


	// 2ο παράδειγμα
	chunkSize = 5;
	bWay = 10;
	fileIterator = 0;
	if (system("rm -f data.db out*.db") == -1) {}
	printf("\nExample 2: Blocks per Chunk = %d | B-Way = %d\n", chunkSize, bWay);

	file_desc = createAndPopulateHeapFile(FILE_NAME);
	sortPhase(file_desc,chunkSize);
	mergePhases(file_desc,chunkSize,bWay,&fileIterator);

	if (fileIterator > 0) {
		char lastfile[50];
		sprintf(lastfile, "%s%d.db", OUT_NAME,fileIterator -1);
		checkIfSorted(lastfile);
	}
	else {
		checkIfSorted(FILE_NAME);
	}


	BF_Close();
	return 0;
}



int createAndPopulateHeapFile(char* filename){
	HP_CreateFile(filename);

	int file_desc;
	HP_OpenFile(filename, &file_desc);

	Record record;
	srand(12569874);
	for (int id = 0; id < RECORDS_NUM; ++id){
		record = randomRecord();
		HP_InsertEntry(file_desc, record);
	}
	return file_desc;
}



/*Performs the sorting phase of external merge sort algorithm on a file specified by 'file_desc', using chunks of size 'chunkSize'*/
void sortPhase(int file_desc,int chunkSize){ 
	sort_FileInChunks(file_desc, chunkSize);
}



/* Performs the merge phase of the external merge sort algorithm  using chunks of size 'chunkSize' and 'bWay' merging. The merge phase may be performed in more than one cycles.*/
void mergePhases(int inputFileDesc,int chunkSize,int bWay, int* fileCounter){
	int oututFileDesc;
	while(chunkSize < HP_GetIdOfLastBlock(inputFileDesc)){
		oututFileDesc = nextOutputFile(fileCounter);
		merge(inputFileDesc, chunkSize, bWay, oututFileDesc );
		HP_CloseFile(inputFileDesc);
		chunkSize*=bWay;
		inputFileDesc = oututFileDesc;
	}
	HP_CloseFile(oututFileDesc);
}



/*Creates a sequence of heap files: out0.db, out1.db, ... and returns for each heap file its corresponding file descriptor. */
int nextOutputFile(int* fileCounter){
	char mergedFile[50];
	char tmp[] = "out";
	sprintf(mergedFile, "%s%d.db", tmp, (*fileCounter)++);
	int file_desc;
	HP_CreateFile(mergedFile);
	HP_OpenFile(mergedFile, &file_desc);
	return file_desc;
}


// Ελέγχει αν ένα αρχείο (όλες οι εγγραφές) είναι ταξινομημένο
void checkIfSorted(char* filename) {
	int file_desc;
	HP_OpenFile(filename,&file_desc);
	int lastBlock = HP_GetIdOfLastBlock(file_desc);
	Record curr,next;

	if (HP_GetRecord(file_desc,1,0,&curr) != 0) {
		printf("Error: Unable to retrieve first record or file is empty.\n");
		HP_Unpin(file_desc,1);
		HP_CloseFile(file_desc);
		return;
	}


	int count = 0;
	bool sorted = true;
	for (int i = 1;i <= lastBlock;i++) {
		int recordsInBlock = HP_GetRecordCounter(file_desc,i);
		for (int j = 0;j < recordsInBlock;j++) {
			HP_GetRecord(file_desc,i,j,&next);

			if (shouldSwap(&curr,&next)) {
				printf("Error: Wrong order between records %s %s and %s %s.\n",curr.name, curr.surname, next.name, next.surname);
				sorted = false;
			}
			curr = next;
			count++;
		}
		HP_Unpin(file_desc,i);
	}
	
	if (sorted) {
		printf("Success: Final file %s is sorted with %d records.\n",filename,count);
	}
	else {
		printf("Error: Final file %s is not sorted in the right way.\n",filename);
	}
	
	HP_CloseFile(file_desc);
}

