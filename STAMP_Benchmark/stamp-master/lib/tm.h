/* =============================================================================
 *
 * tm.h
 *
 * Utility defines for transactional memory
 *
 * =============================================================================
 *
 * Copyright (C) Stanford University, 2006.  All Rights Reserved.
 * Authors: Chi Cao Minh and Martin Trautmann
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


#ifndef TM_H
#define TM_H

#ifdef HAVE_CONFIG_H
# include "STAMP_config.h"
#endif
#include<pthread.h>
#include<setjmp.h>
#include <string.h>
#include "../../tl2/stm.h"
#include "thread.h"
#include <map>

extern __thread int tid; // or -1

//template <typename T> T


/* =============================================================================
 * Simulator Specific Interface
 *
 * MAIN(argc, argv)
 *     Declare the main function with argc being the identifier for the argument
 *     count and argv being the name for the argument string list
 *
 * MAIN_RETURN(int_val)
 *     Returns from MAIN function
 *
 * GOTO_SIM()
 *     Switch simulator to simulation mode
 *
 * GOTO_REAL()
 *     Switch simulator to non-simulation (real) mode
 *     Note: use in sequential region only
 *
 * IS_IN_SIM()
 *     Returns true if simulator is in simulation mode
 *
 * SIM_GET_NUM_CPU(var)
 *     Assigns the number of simulated CPUs to "var"
 *
 * P_MEMORY_STARTUP
 *     Start up the memory allocator system that handles malloc/free
 *     in parallel regions (but not in transactions)
 *
 * P_MEMORY_SHUTDOWN
 *     Shutdown the memory allocator system that handles malloc/free
 *     in parallel regions (but not in transactions)
 *
 * =============================================================================
 */


#  include <stdio.h>

#  define MAIN(argc, argv)              int main (int argc, char** argv)
#  define MAIN_RETURN(val)              return val

#  define GOTO_SIM()                    /* nothing */
#  define GOTO_REAL()                   /* nothing */
#  define IS_IN_SIM()                   (0)

#  define SIM_GET_NUM_CPU(var)          /* nothing */

#  define TM_PRINTF                     printf
#  define TM_PRINT0                     printf
#  define TM_PRINT1                     printf
#  define TM_PRINT2                     printf
#  define TM_PRINT3                     printf

#  define P_MEMORY_STARTUP(numThread)   /* nothing */
#  define P_MEMORY_SHUTDOWN()           /* nothing */




/* =============================================================================
 * Transactional Memory System Interface
 *
 * TM_ARG
 * TM_ARG_ALONE
 * TM_ARGDECL
 * TM_ARGDECL_ALONE
 *     Used to pass TM thread meta data to functions (see Examples below)
 *
 * TM_STARTUP(numThread)
 *     Startup the TM system (call before any other TM calls)
 *
 * TM_SHUTDOWN()
 *     Shutdown the TM system
 *
 * TM_THREAD_ENTER()
 *     Call when thread first enters parallel region
 *
 * TM_THREAD_EXIT()
 *     Call when thread exits last parallel region
 *
 * P_MALLOC(size)
 *     Allocate memory inside parallel region
 *
 * P_FREE(ptr)
 *     Deallocate memory inside parallel region
 *
 * TM_MALLOC(size)
 *     Allocate memory inside atomic block / transaction
 *
 * TM_FREE(ptr)
 *     Deallocate memory inside atomic block / transaction
 *
 * TM_BEGIN()
 *     Begin atomic block / transaction
 *
 * TM_BEGIN_RO()
 *     Begin atomic block / transaction that only reads shared data
 *
 * TM_END()
 *     End atomic block / transaction
 *
 * TM_RESTART()
 *     Restart atomic block / transaction
 *
 * TM_EARLY_RELEASE()
 *     Remove speculatively read line from the read set
 *
 * =============================================================================
 *
 * Example Usage:
 *
 *     MAIN(argc,argv)
 *     {
 *         TM_STARTUP(8);
 *         // create 8 threads and go parallel
 *         TM_SHUTDOWN();
 *     }
 *
 *     void parallel_region ()
 *     {
 *         TM_THREAD_ENTER();
 *         subfunction1(TM_ARG_ALONE);
 *         subfunction2(TM_ARG  1, 2, 3);
 *         TM_THREAD_EXIT();
 *     }
 *
 *     void subfunction1 (TM_ARGDECL_ALONE)
 *     {
 *         TM_BEGIN_RO()
 *         // ... do work that only reads shared data ...
 *         TM_END()
 *
 *         long* array = (long*)P_MALLOC(10 * sizeof(long));
 *         // ... do work ...
 *         P_FREE(array);
 *     }
 *
 *     void subfunction2 (TM_ARGDECL  long a, long b, long c)
 *     {
 *         TM_BEGIN();
 *         long* array = (long*)TM_MALLOC(a * b * c * sizeof(long));
 *         // ... do work that may read or write shared data ...
 *         TM_FREE(array);
 *         TM_END();
 *     }
 *
 * =============================================================================
 */


/* =============================================================================
 * HTM - Hardware Transactional Memory
 * =============================================================================
 */


/* =============================================================================
 * STM - Software Transactional Memory
 * =============================================================================
 */




#    define TM_ARG                        STM_SELF,
#    define TM_ARG_ALONE                  STM_SELF
#    define TM_ARGDECL                    STM_THREAD_T* TM_ARG
#    define TM_ARGDECL_ALONE              STM_THREAD_T* TM_ARG_ALONE
#    define TM_CALLABLE                   /* nothing */

//#      define TM_STARTUP(numThread)     STM_STARTUP(numThread)
//#      define TM_SHUTDOWN()             tm_shutdown()

#      define TM_THREAD_ENTER()         TM_ARGDECL_ALONE = STM_NEW_THREAD(); \
                                        STM_INIT_THREAD(TM_ARG_ALONE, thread_getId()); \
                                        //tid = -1;
#      define TM_THREAD_EXIT()          STM_FREE_THREAD(TM_ARG_ALONE)

#      define P_MALLOC(size)            malloc(size)
#      define P_FREE(ptr)               free(ptr)
#      define TM_MALLOC(size)           malloc(size)
//#      define TM_FREE(ptr)              STM_FREE(ptr)


//#    define TM_BEGIN()               STM_BEGIN_WR(&tid)
//#    define TM_BEGIN_RO()               STM_BEGIN_RD()
//#    define TM_END()                 STM_END(tid)
//#    define TM_RESTART()                STM_RESTART()

#    define TM_EARLY_RELEASE(var)       /* nothing */



/* =============================================================================
 * Transactional Memory System interface for shared memory accesses
 *
 * There are 3 flavors of each function:
 *
 * 1) no suffix: for accessing variables of size "long"
 * 2) _P suffix: for accessing variables of type "pointer"
 * 3) _F suffix: for accessing variables of type "float"
 * =============================================================================
 */

/*#define FUNC_DECL(x, y)			\
pc.push_back(x);						\
c##x: y;									\
pc.pop_back();

//#  define TM_SHARED_READ(T, var)           *((T *)tm_read(&var, tid, sizeof(var)))
#  define TM_SHARED_READ(var)           STM_READ(&(var), tid, sizeof(var))
//#  define TM_SHARED_READ(var1)		0 
#  define TM_SHARED_READ_P(var)         STM_READ_P(&(var), tid, sizeof(var))
#  define TM_SHARED_READ_F(var)         STM_READ(&(var), tid, sizeof(var))

//#  define TM_SHARED_WRITE(var1, var2, var3, var4)     STM_WRITE(var1, var2, tid, sizeof(var1))
#  define TM_SHARED_WRITE_P(var, val)   tm_write_p(&var, (val), (int)tid, sizeof(var))
#  define TM_SHARED_WRITE(var, val)		tm_write(&var, (val), (int)tid, sizeof(var))
#  define TM_SHARED_WRITE_F(var, val)   tm_write(&var, (val), (int)tid, sizeof(var))

#  define TM_LOCAL_WRITE(var, val)      STM_LOCAL_WRITE(var, val)
#  define TM_LOCAL_WRITE_P(var, val)    STM_LOCAL_WRITE(var, val)
#  define TM_LOCAL_WRITE_F(var, val)    STM_LOCAL_WRITE_F(var, val)

template <typename T> 
void tm_write(T*, T, int, size_t );

template <typename T> 
void tm_write_p(T**, T*, int, size_t );

template <typename T> 
void tm_write(T* addr1, T addr2, int tid, size_t size){
	//static __thread T addr3;
	//addr3 = addr2;
	STM_WRITE(addr1, &addr2, tid, size);
}

template <typename T> 
void tm_write_p(T** addr1, T* addr2, int tid, size_t size){
	//static __thread T* addr3;
	//addr3 = addr2;	
	STM_WRITE( addr1,  &addr2, tid, size);
}

#define MACR 						\
	int val = setjmp(env);			\
	if(rollback == 1){				\
		int t = pr.front();			\
		pr.pop_front();				\
		pc.push_back(t);			\
		if(pr.empty())				\
			rollback = 0;			\
		goto *array[t];				\
	}								\
	
	

#define MACR1 						\
	if(rollback == 1){				\
		int t = pr.front();			\
		pr.pop_front();				\
		pc.push_back(t);			\
		if(pr.empty())				\
			rollback = 0;			\
		goto *array[t];				\
	}								\
*/
#endif /* TM_H */


/* =============================================================================
 *
 * End of tm.h
 *
 * =============================================================================
 */

