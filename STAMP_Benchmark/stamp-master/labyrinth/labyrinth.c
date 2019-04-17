/* =============================================================================
 *
 * labyrinth.c
 *
 * =============================================================================
 *
 * Copyright (C) Stanford University, 2006.  All Rights Reserved.
 * Author: Chi Cao Minh
 *
 * =============================================================================
 *
 * For the license of bayes/sort.h and bayes/sort.c, please see the header
 * of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of kmeans, please see kmeans/LICENSE.kmeans
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of ssca2, please see ssca2/COPYRIGHT
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/mt19937ar.c and lib/mt19937ar.h, please see the
 * header of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/rbtree.h and lib/rbtree.c, please see
 * lib/LEGALNOTICE.rbtree and lib/LICENSE.rbtree
 * 
 * ------------------------------------------------------------------------
 * 
 * Unless otherwise noted, the following license applies to STAMP files:
 * 
 * Copyright (c) 2007, Stanford University
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 * 
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 * 
 *     * Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 * =============================================================================
 */


#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include "list.h"
#include "maze.h"
#include "router.h"
#include "thread.h"
#include "timer.h"
#include "types.h"
#include <iostream>
map<long int*, long int> *MAP;
//Create an instance of KSFTM here.
	KSFTM *lib;

enum param_types {
    PARAM_BENDCOST = (unsigned char)'b',
    PARAM_THREAD   = (unsigned char)'t',
    PARAM_XCOST    = (unsigned char)'x',
    PARAM_YCOST    = (unsigned char)'y',
    PARAM_ZCOST    = (unsigned char)'z',
};

enum param_defaults {
    PARAM_DEFAULT_BENDCOST = 1,
    PARAM_DEFAULT_THREAD   = 64,
    PARAM_DEFAULT_XCOST    = 1,
    PARAM_DEFAULT_YCOST    = 1,
    PARAM_DEFAULT_ZCOST    = 2,
};

bool_t global_doPrint = FALSE;
char* global_inputFile = NULL;
long global_params[256]; /* 256 = ascii limit */


/* =============================================================================
 * displayUsage
 * =============================================================================
 */
static void
displayUsage (const char* appName)
{
    printf("Usage: %s [options]\n", appName);
    puts("\nOptions:                            (defaults)\n");
    printf("    b <INT>    [b]end cost          (%i)\n", PARAM_DEFAULT_BENDCOST);
    printf("    i <FILE>   [i]nput file name    (%s)\n", global_inputFile);
    printf("    p          [p]rint routed maze  (false)\n");
    printf("    t <UINT>   Number of [t]hreads  (%i)\n", PARAM_DEFAULT_THREAD);
    printf("    x <UINT>   [x] movement cost    (%i)\n", PARAM_DEFAULT_XCOST);
    printf("    y <UINT>   [y] movement cost    (%i)\n", PARAM_DEFAULT_YCOST);
    printf("    z <UINT>   [z] movement cost    (%i)\n", PARAM_DEFAULT_ZCOST);
    exit(1);
}


/* =============================================================================
 * setDefaultParams
 * =============================================================================
 */
static void
setDefaultParams ()
{
    global_params[PARAM_BENDCOST] = PARAM_DEFAULT_BENDCOST;
    global_params[PARAM_THREAD]   = PARAM_DEFAULT_THREAD;
    global_params[PARAM_XCOST]    = PARAM_DEFAULT_XCOST;
    global_params[PARAM_YCOST]    = PARAM_DEFAULT_YCOST;
    global_params[PARAM_ZCOST]    = PARAM_DEFAULT_ZCOST;
}


/* =============================================================================
 * parseArgs
 * =============================================================================
 */
static void
parseArgs (long argc, char* const argv[])
{
    long i;
    long opt;

    opterr = 0;

    setDefaultParams();

    while ((opt = getopt(argc, argv, "b:i:pt:x:y:z:")) != -1) {
        switch (opt) {
            case 'b':
            case 't':
            case 'x':
            case 'y':
            case 'z':
                global_params[(unsigned char)opt] = atol(optarg);
                break;
            case 'i':
                global_inputFile = optarg;
                break;
            case 'p':
                global_doPrint = TRUE;
                break;
            case '?':
            default:
                opterr++;
                break;
        }
    }

    for (i = optind; i < argc; i++) {
        fprintf(stderr, "Non-option argument: %s\n", argv[i]);
        opterr++;
    }

    if (opterr) {
        displayUsage(argv[0]);
    }
}


/* =============================================================================
 * main
 * =============================================================================
 */
MAIN(argc, argv)
{
	MAP = new map<long int*, long int>();
	
    GOTO_REAL();

    /*
     * Initialization
     */
    parseArgs(argc, (char** const)argv);
    long numThread = global_params[PARAM_THREAD];
    SIM_GET_NUM_CPU(numThread);
    //TM_STARTUP(numThread);
    P_MEMORY_STARTUP(numThread);
    thread_startup(numThread);
    maze_t* mazePtr = maze_alloc();
    assert(mazePtr);
    long numPathToRoute = maze_read(mazePtr, global_inputFile);
    router_t* routerPtr = router_alloc(global_params[PARAM_XCOST],
                                       global_params[PARAM_YCOST],
                                       global_params[PARAM_ZCOST],
                                       global_params[PARAM_BENDCOST]);
    assert(routerPtr);
    list_t* pathVectorListPtr = list_alloc(NULL);
    assert(pathVectorListPtr);
		

	
	long int k = 0;
	
	// workQueuePtr -> QUEUE first SHARED OBJECT.
	MAP->insert(std::pair<long int*, long int>((long int*)&(mazePtr->workQueuePtr->pop), k));
	k++;
	MAP->insert(std::pair<long int*, long int>((long int*)&(mazePtr->workQueuePtr->push), k));
	k++;
	MAP->insert(std::pair<long int*, long int>((long int*)&(mazePtr->workQueuePtr->capacity), k));
	k++;
	
	//cout<<((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[0]))->secondPtr))->x<<endl;
	int queue_elements =  sizeof(mazePtr->workQueuePtr->elements) / sizeof (mazePtr->workQueuePtr->elements[0]);  
	std::cout<<"QUEUE SIZE"<<mazePtr->workQueuePtr->push<<endl;
	
	
	for(long int i=0;i<mazePtr->workQueuePtr->push;i++)
	{
		MAP->insert(std::pair<long int*, long int>((long int*)&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->x), k));
		k++;
		MAP->insert(std::pair<long int*, long int>((long int*)&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->y), k));
		k++;
		MAP->insert(std::pair<long int*, long int>((long int*)&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->z), k));
		k++;
		MAP->insert(std::pair<long int*, long int>((long int*)&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->x), k));
		k++;
		MAP->insert(std::pair<long int*, long int>((long int*)&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->y), k));
		k++;
		MAP->insert(std::pair<long int*, long int>((long int*)&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->z), k));
		k++;
	}
	
	
	//mazePtr->gridPtr -> GET the GRID SHARED OBJECT
	long width = mazePtr->gridPtr->width;
	long height = mazePtr->gridPtr->height;
	long depth = mazePtr->gridPtr->depth;
	
	for (long z = 0; z < depth; z++) {
		for (long x = 0; x < width; x++) {
			for (long y = 0; y < height; y++) {
                MAP->insert(std::pair<long int*, long int>((long int*)grid_getPointRef(mazePtr->gridPtr, x, y, z), k));
                k++;
            }
        }
    }
	
	//pathVectorListPtr : Get the shared list size equal to the number of threads.(numThread)
	for(int i=0;i<numThread;i++)
	{
		vector_t* pointVectorPtr = new vector_t();
		list_insert (pathVectorListPtr, pointVectorPtr);
	}
	
	// Get the total path insert shared variable. As numPathRouted.
	long *tm_numPathRouted;
	MAP->insert(std::pair<long int*, long int>(tm_numPathRouted, k));
	
	// Initialize KSFTM instance.
	lib = new KSFTM(k+1);
	

	// INITIALIZE THE DATA VALUES IN THE KSFTM'S INSTANCE.
	LTransaction* T1 = new LTransaction;
	T1->g_its = NIL;
	while(true) {
		if(T1->g_its != NIL) {
			long int its = T1->g_its;
			T1 = lib->tbegin(its);
		} else {
			T1 = lib->tbegin(T1->g_its);
		}
		
		TobIdValPair *tobj_id_val_pair = new TobIdValPair;
		tobj_id_val_pair->id = MAP->at(&(mazePtr->workQueuePtr->push));
		tobj_id_val_pair->val = mazePtr->workQueuePtr->push;
		lib->stmWrite(T1, tobj_id_val_pair);
		
		TobIdValPair *tobj_id_val_pair1 = new TobIdValPair;
		tobj_id_val_pair1->id = MAP->at(&(mazePtr->workQueuePtr->pop));
		tobj_id_val_pair1->val = mazePtr->workQueuePtr->pop;
		lib->stmWrite(T1, tobj_id_val_pair1);
		
		TobIdValPair *tobj_id_val_pair2 = new TobIdValPair;
		tobj_id_val_pair2->id = MAP->at(&(mazePtr->workQueuePtr->capacity));
		tobj_id_val_pair2->val = mazePtr->workQueuePtr->capacity;
		lib->stmWrite(T1, tobj_id_val_pair2);
		
		for(long int i=0;i<mazePtr->workQueuePtr->push;i++)
		{
			TobIdValPair *tobj_id_val_pair = new TobIdValPair;
			tobj_id_val_pair->id = MAP->at(&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->x));
			long int x1 = ((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->x;
			tobj_id_val_pair->val = x1;
			lib->stmWrite(T1, tobj_id_val_pair);
			
			TobIdValPair *tobj_id_val_pair1 = new TobIdValPair;
			tobj_id_val_pair1->id = MAP->at(&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->y));
			long int y1 = ((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->y;
			tobj_id_val_pair1->val = y1;
			lib->stmWrite(T1, tobj_id_val_pair1);

			TobIdValPair *tobj_id_val_pair2 = new TobIdValPair;
			tobj_id_val_pair2->id = MAP->at(&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->z));
			long int z1 = ((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->firstPtr))->z;
			tobj_id_val_pair2->val = z1;
			lib->stmWrite(T1, tobj_id_val_pair2);
			
			TobIdValPair *tobj_id_val_pair3 = new TobIdValPair;
			tobj_id_val_pair3->id = MAP->at(&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->x));
			long int x2 = ((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->x;
			tobj_id_val_pair3->val = x2;
			lib->stmWrite(T1, tobj_id_val_pair3);
			
			TobIdValPair *tobj_id_val_pair4 = new TobIdValPair;
			tobj_id_val_pair4->id = MAP->at(&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->y));
			long int y2 = ((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->y;
			tobj_id_val_pair4->val = y2;
			lib->stmWrite(T1, tobj_id_val_pair4);
			
			TobIdValPair *tobj_id_val_pair5 = new TobIdValPair;
			tobj_id_val_pair5->id = MAP->at(&(((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->z));
			long int z2 = ((coordinate_t*)(((pair_t*)(mazePtr->workQueuePtr->elements[i]))->secondPtr))->z;
			tobj_id_val_pair5->val = z2;
			lib->stmWrite(T1, tobj_id_val_pair5);
			
		}
		
		for (long z = 0; z < depth; z++) {
			for (long x = 0; x < width; x++) {
				for (long y = 0; y < height; y++) {
					//MAP->insert(std::pair<long int*, long int>(grid_getPointRef(mazePtr->gridPtr, x, y, z), k));
					TobIdValPair *tobj_id_val_pair = new TobIdValPair;
					tobj_id_val_pair->id = MAP->at(grid_getPointRef(mazePtr->gridPtr, x, y, z));
					tobj_id_val_pair->val = *grid_getPointRef(mazePtr->gridPtr, x, y, z);
					lib->stmWrite(T1, tobj_id_val_pair);
				}
			}
		}	
		
		
		/* Try to commit the current transaction. If stmTryCommit returns ABORTED then retry this transaction again. */
		if(lib->stmTryCommit(T1) == ABORTED) {
			continue;
		}
		// Break out of the while loop since the transaction has committed
		break;
	}
	
	
	
	/*
     * Run transactions
     */
    router_solve_arg_t routerArg = {lib, MAP, routerPtr, mazePtr, pathVectorListPtr, tm_numPathRouted};
    
   
    
    TIMER_T startTime;
    TIMER_READ(startTime);
    GOTO_SIM();
#ifdef OTM
#pragma omp parallel
    {
        router_solve((void *)&routerArg);
    }
#else
    thread_start(router_solve, (void*)&routerArg);
#endif
    GOTO_REAL();
    TIMER_T stopTime;
    TIMER_READ(stopTime);


	 float max_time = 0.0;
	 float timeTotal = 0.0;
    
    for(int i=0;i<numThread;i++)
    {
		if(routerArg.time[i] > max_time)
		 max_time = routerArg.time[i];
		 timeTotal += routerArg.totalTime[thread_getId()];
	}
	
    long numPathRouted = 0;
    /*list_iter_t it;
    list_iter_reset(&it, pathVectorListPtr);
    while (list_iter_hasNext(&it, pathVectorListPtr)) {
        vector_t* pathVectorPtr = (vector_t*)list_iter_next(&it, pathVectorListPtr);
        numPathRouted += vector_getSize(pathVectorPtr);
    }*/
    
    LTransaction* T = new LTransaction;
	T->g_its = NIL;
	
	label: while(true) {
		if(T->g_its != NIL) {
			long int its = T->g_its;
			T = lib->tbegin(its);
		} else {
			T = lib->tbegin(T->g_its);
		}
		
		TobIdValPair *tobj_id_val_pair = new TobIdValPair;
		tobj_id_val_pair->id = MAP->at(tm_numPathRouted);
		if(lib->stmRead(T, tobj_id_val_pair) == ABORTED)
		{
			goto label;
		}
		numPathRouted = tobj_id_val_pair->val;

		/* Try to commit the current transaction. If stmTryCommit returns ABORTED then retry this transaction again. */
		if(lib->stmTryCommit(T) == ABORTED) {
			continue;
		}
		// Break out of the while loop since the transaction has committed
		break;
	}
    
    printf("Paths routed    = %li\n", numPathRouted);
    printf("Elapsed time    = %f seconds, Worst Maximum Time = %f : Average Time = %f\n", TIMER_DIFF_SECONDS(startTime, stopTime),max_time,timeTotal/numThread);

    /*
     * Check solution and clean up
     */
    assert(numPathRouted <= numPathToRoute);
    bool_t status = maze_checkPaths(mazePtr, pathVectorListPtr, global_doPrint);
    assert(status == TRUE);
    puts("Verification passed.");
    maze_free(mazePtr);
    router_free(routerPtr);

   // TM_SHUTDOWN();
    P_MEMORY_SHUTDOWN();

    GOTO_SIM();

    thread_shutdown();


    MAIN_RETURN(0);
}


/* =============================================================================
 *
 * End of labyrinth.c
 *
 * =============================================================================
 */
