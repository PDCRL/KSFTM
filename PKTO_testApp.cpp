//  PKTO_testApp.cpp
//
//  Created by PDCRL group on 15/1/19..
//  Copyright © 2019 IIT-HYD. All rights reserved.


#include <sys/time.h>
#include <unistd.h>
#include <random>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include "PKTO.cpp"
# include <mutex>

#define READ 0
#define WRITE 1

#define OP_DEL 100
#define WRITE_VAL 1000
#define OP_SEED 100

#define T_OBJ_SEED 5
#define OP_LT_SEED 10

#define NUM_THREADS 250
#define TRANS_LT NUM_THREADS 

#define READ_PER 10

using namespace std;

double timee[NUM_THREADS];

PKTO* lib = new PKTO(T_OBJ_SEED);
//Transaction Limit atomic counter
atomic<int> transLt;

atomic<int> RabortCnt;
atomic<int> WabortCnt;

double timeRequest() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  double timevalue = tp.tv_sec + (tp.tv_usec/1000000.0);
  return timevalue;
}

class TestAppln {
	
	int numOps, readPer, opDel;
	int opSeed, opLtSeed, tobjSeed, writeVal;
	int currTObj,randVal,currOp;
	
	public:
	TestAppln();
	int testFunc();	
};

TestAppln::TestAppln() {
	
	readPer = READ_PER;
	opDel = OP_DEL;
	opSeed = OP_SEED;
	opLtSeed = OP_LT_SEED;
	tobjSeed = T_OBJ_SEED;
	writeVal = WRITE_VAL;
}
int TestAppln::testFunc() {
	
	LTransaction* T = new LTransaction;
	T->g_its = NIL;
	
	int localAbortCnt = 0;
	
	label: while(true) {
		if(T->g_its != NIL) {
			long int its = T->g_its;
			T = lib->tbegin(its);
		} else {
			T = lib->tbegin(T->g_its);
		}
		
		// Generate the number of operations to execute in this transaction
		numOps = rand()%opLtSeed;
		for(int opCnt = 0; opCnt < 10; opCnt++) {
		/* Generate the tobj on which the next tobj will execute
					returns a random value in the range 1 - tobjSeed*/
			currTObj = rand()%tobjSeed;

			/* Generate the next operation based on the percentage
			   Generate a random value between 0 - 100 */
			randVal = rand()%opSeed;
			if (randVal <= readPer) {
				currOp = READ;
			} else {
				currOp = WRITE;
			}					
			// Take the appropriate action depending on the current operation
			TobIdValPair *tobj_id_val_pair = new TobIdValPair;
			switch(currOp) {
				case READ:
					tobj_id_val_pair->id = currTObj;
					// If the read operation aborts then restart the transaction
					if(lib->stmRead(T, tobj_id_val_pair) == ABORTED) {
						RabortCnt++;
						localAbortCnt++;
						goto label;
					}
					break;
				
				case WRITE:
					tobj_id_val_pair->id = currTObj;
					tobj_id_val_pair->val = rand()%writeVal;
					lib->stmWrite(T,tobj_id_val_pair);
					break;
			}
			// Sleep for random time between each operation. Simulates the thread performing some operation
			//usleep(rand()%opDel);
		}// End for numOps
		
		/* Try to commit the current transaction.
		   If stmTryCommit returns ABORTED then retry this transaction again. */
		if(lib->stmTryCommit(T) == ABORTED) {
			WabortCnt++;
			localAbortCnt++;
			continue;
		}
		// Break out of the while loop since the transaction has committed
		break;
	} // End while true			
		
	return localAbortCnt;
}// End TestFunc
	
void* testFunc_helper(void *ptr_id)
{
	int id = *((int*)ptr_id);
	double btime,etime;
	int transLmt = transLt.fetch_add(ONE);
	TestAppln* testAppl = new TestAppln;
	
	// Execute this loop until the transLt number of transactions execute successfully
	while(transLmt < TRANS_LT) {
		//begin time
		btime = timeRequest();
		
		int localAbortCnt = testAppl->testFunc();
		
		//end time
		etime = timeRequest();
		
		timee[id] = timee[id] + (etime - btime);
		
		transLmt = transLt.fetch_add(ONE);
	}// End for transLt
}
	
int main()
{
	double sumTime=0.0,btime,etime;
	long readAbort = 0, writeAbort =0;
	
	
	int threadId[NUM_THREADS], k = 0;
	while(k<NUM_THREADS)
	{
		timee[k] = 0;
		threadId[k] = k;
		k++;
	} 
	transLt.store(ZERO);
	RabortCnt.store(ZERO);
	WabortCnt.store(ZERO);
	
	pthread_t threads[NUM_THREADS];
	int loop =0;
	//while(loop<5) {
	
	//btime = timeRequest();
		
		for (int i=0; i < NUM_THREADS; i++) {
			pthread_create(&threads[i], NULL, testFunc_helper, &threadId[i]);
		}
		
		//only after all the threads join, the parent has to exit
		for(int i=0; i< NUM_THREADS; i++) {
			  pthread_join(threads[i],NULL); 
		}
	//etime = timeRequest();		
		loop++;
		k = 0;
		double max_time = 0.0;
		while(k<NUM_THREADS)
		{
			if(max_time<timee[k])
				max_time = timee[k];
			k++;
		}
		
		readAbort += RabortCnt.load();
		writeAbort += WabortCnt.load();
		transLt.store(ZERO);
		RabortCnt.store(ZERO);
		WabortCnt.store(ZERO);
	//}	
	
	cout<<"Worst time calculated "<<max_time<<endl;
	
	
	//cout<<"\nAverage Time for 5 iterations "<<total_time/1.0<<" RAbrtCnt "<<readAbort/1<<" WAbrtCnt  "<<writeAbort/1<<" Total Abort "<<readAbort+writeAbort<<endl;
	//cout<<"Total memory allocated to the versions is -> "<<totalVersions<<"\nTotal memory allocated to the read list nodes is ->"<<totalReadListNodes<<endl;
	pthread_exit(NULL);
    
    return 0;
}
