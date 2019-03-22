//  PKTO.cpp
//
//  Created by PDCRL group on 15/1/19..
//  Copyright © 2019 IIT-HYD. All rights reserved.



#include "PKTO.h"
#define OK true
#define ABORTED false
#define K 5
#define NIL -1
#define TRUE true
#define FALSE false
#define ZERO 0
#define ONE 1
#define INFINITE 99999999;
#define C 0.1
/**************************** CONSTRUCTORS *****************************/
/*
 * Global Transaction(GTransaction) class constructor
 * */
GTransaction::GTransaction()
{
	g_valid = TRUE;
	g_lock = new mutex;
}

/*
 * Transaction object(Tobj) class constructor
 * */
Tobj::Tobj()
{
	tobj_lock = new mutex;
}

/*
 * Constructor of the class PKTO which performs the initialize operation. 
 * Invoked at the start of the STM system. Initializes all the tobjs used by the STM System.
 * */
PKTO::PKTO(int INITIAL_objs)
{
	g_tCntr.store(ONE);
	
	// For all the tobjs used by the STM System
	for(int i=ZERO;i<INITIAL_objs;i++) {
		
		//version created by transaction T0
		Version *ver_T0 = new Version;
		ver_T0->cts = ZERO;
		ver_T0->val = ZERO;
						
		/*Create transaction object and push above created temporary
			Version on to its VersionList*/	
		Tobj *tobj = new Tobj;
		tobj->k = ONE;
		tobj->versionList->push_back(ver_T0);

		//Add 1 to the total versions allocated memory log counter.
		totalVersions.fetch_add(1);
		
		//Push the transaction object created on the transaction objects list	
		tobjs->push_back(*tobj); 
	}
}

/************************ STM::PRIVATE METHODS ***********************/
/*
 * Returns the largest ts value less than g_wts of the invoking transaction. 
 * If no such version exists, it returns nil.
 * */
Version* PKTO::findLTS_STL(long int g_cts, long int tobj_id)
{
	Version *curVer = NULL;
	list<Version*>::iterator VL_iterator;
	Version *ver_iterator;
	
	VL_iterator = tobjs->at(tobj_id).versionList->begin();
	while(VL_iterator != tobjs->at(tobj_id).versionList->end())
    {
		ver_iterator = *VL_iterator;
		
		if(ver_iterator->cts < g_cts) {
			curVer = ver_iterator;
		}
		VL_iterator++;
	}
	return curVer;
}

/************************ PKTO::PRIVATE METHODS ***********************/

/*
 * Method to search for a transaction object in the 'set' passes 
 * as an argument to the function. 
 * */	
bool PKTO::find_set(vector<TobIdValPair> *set, TobIdValPair* tobj_id_val_pair)
{
	if(tobj_id_val_pair != NULL && set != NULL) {
		for(int i = ZERO;i<set->size();i++) {
			//Check if transaction object of 'tobj_id_val_pair->id' present in the set
			if(set->at(i).id == tobj_id_val_pair->id) {
				//set the value corresponding to the tobject in the 'tobj_id_val_pair' instance pointer.
				tobj_id_val_pair->val = set->at(i).val;
				//return OK
				return TRUE;
			}
		}
	}
	//else return not found/false
	return FALSE;
}

/*
 * Insert a transaction in the reader's list of a version of a transaction object.
 * */
void PKTO::insertAndSortRL(list<GTransaction*> *RL, GTransaction* gtran)
{
	bool insertFlag = FALSE;
	GTransaction *gtran_iterator;
	list<GTransaction*>::iterator gtran_list_iterator;
	
	
	if(gtran != NULL && RL != NULL) {
		/*Optimization check : If transaction  to be inserted in the RL is aborted or its 
		 * valid flag is false we will not insert such transactions in the list*/
		if(isAborted(gtran)) {
			return;
		}
		//If the reader's list is empty push the gtrans to the reader's list
		if(RL->size() == ZERO) {
			//Add 1 to the total versions allocated memory for read list nodes log counter.
			totalReadListNodes.fetch_add(1);
			RL->push_back(gtran);
			return;
		} 
		//If the reader's list is not empty and has elements more than ONE.
		gtran_list_iterator = RL->begin();
		while(gtran_list_iterator != RL->end())
		{
			gtran_iterator = *gtran_list_iterator;
							
			/*if reader's list transaction is greater than passed 
				transaction's wts value insert the transaction*/
			if(gtran->g_cts < gtran_iterator->g_cts) {
				insertFlag = TRUE;
				break;
			}
			//if cts are same then element already exist; exclude redundancy 
			else if(gtran->g_cts == gtran_iterator->g_cts) {
				return;
			}
			gtran_list_iterator++;
		}
		
		//if not inserted, insert it in the last of the list
		if(insertFlag == FALSE) {
			RL->push_back(gtran);
			//Add 1 to the total versions allocated memory for read list nodes log counter.
			totalReadListNodes.fetch_add(1);
		} else {
			RL->insert(gtran_list_iterator,gtran);
			//Add 1 to the total versions allocated memory for read list nodes log counter.
			totalReadListNodes.fetch_add(1);
		}
	}	 
}

/*
 * Insert the version and maintain the ascending order of wts value of versions of the version list
 * */
void PKTO::insertAndSortVL(Version *version, long int objId)
{
	//string str = to_string(objId)+"->"+" New Element "+to_string(version->wts)+" ";
	
	bool insertFlag = FALSE;
	list<Version*>::iterator VL_iterator;
	Version *ver_iterator;
	
	if(version != NULL) {	
		//if there is no version apart from zero'th version and K is greater than 1.
		if(tobjs->at(objId).k==ONE && tobjs->at(objId).k != K) {
			insertFlag = FALSE;
		}
		 
		VL_iterator = tobjs->at(objId).versionList->begin();
		
		//if transaction's object K versions exists than overwrite the oldest version
		if(tobjs->at(objId).k >= K && tobjs->at(objId).versionList->size() > 0) {
			//log the total read lists nodes to be deleted.
			totalReadListNodes.fetch_sub((*VL_iterator)->rl->size());
			//erase the first version of the version list
			VL_iterator = tobjs->at(objId).versionList->erase(VL_iterator);
			//Subtract 1 from the total versions allocated memory log counter.
			totalVersions.fetch_sub(1);
					
			while(VL_iterator != tobjs->at(objId).versionList->end())
			{
				ver_iterator = *VL_iterator;
						
				//insert current version
				if(ver_iterator->cts > version->cts) {
					insertFlag = TRUE;
					break;
				}
				VL_iterator++;
			}
		} 
		/*if version list size is less than K, then insert the new version 
			in version list without disturbing the increasing sorted order of the version list*/ 
		else {
			while(VL_iterator != tobjs->at(objId).versionList->end())
			{
				ver_iterator = *VL_iterator;
				if(ver_iterator->cts > version->cts) {
					insertFlag = TRUE;
					break;
				}
				VL_iterator++; 
			}	
		}
		if(insertFlag == FALSE) {
				tobjs->at(objId).versionList->push_back(version);
				//Add 1 to the total versions allocated memory log counter.
				totalVersions.fetch_add(1);
				tobjs->at(objId).k++;
			} else {
				tobjs->at(objId).versionList->insert(VL_iterator,version);
				//Add 1 to the total versions allocated memory log counter.
				totalVersions.fetch_add(1);
				tobjs->at(objId).k++;
		}
	}
}



/*
 * obtain the list of reading transactions whose g_wts are
 * greater than 'g_wts' of the method invoking transaction.
 * 
 * g_wts - g_wts of the invoking transaction
 * preVerRL - Readers list
 * */
list<GTransaction*>* PKTO::getLar(long int g_cts, list<GTransaction*> *preVerRL)
{
	list<GTransaction*> *preVerRL_GT = NULL;
	GTransaction *gtran_iterator;
	list<GTransaction*>::iterator gtran_list_iterator;
	
	if(preVerRL != NULL) {	
		gtran_list_iterator = preVerRL->begin();
		while(gtran_list_iterator != preVerRL->end())
		{
			gtran_iterator = *gtran_list_iterator;
			if(g_cts < gtran_iterator->g_cts) {
				if(preVerRL_GT == NULL) {
					preVerRL_GT = new list<GTransaction*>;
				}
				preVerRL_GT->push_back(gtran_iterator);
			}
			gtran_list_iterator++; 
		}
	}
	return preVerRL_GT;
}

/*
 * obtain the list of reading transactions whose g_wts are
 * smaller than 'g_wts' of the method invoking transaction.
 * 
 * g_wts - g_wts of the invoking transaction
 * preVerRL - Readers list
 * */
list<GTransaction*>* PKTO::getSm(long int g_cts, list<GTransaction*> *preVerRL)
{
	list<GTransaction*> *prevVerRL_ST = NULL;
	GTransaction *gtran_iterator;
	list<GTransaction*>::iterator gtran_list_iterator;
	
	if(preVerRL !=NULL) {
		gtran_list_iterator = preVerRL->begin();
		while(gtran_list_iterator != preVerRL->end())
		{
			gtran_iterator = *gtran_list_iterator;
			if(g_cts > gtran_iterator->g_cts) {
				if(prevVerRL_ST == NULL) {
					prevVerRL_ST = new list<GTransaction*>;
				}
				prevVerRL_ST->push_back(gtran_iterator);
			}
			gtran_list_iterator++; 
		}
	}
	return prevVerRL_ST;
}

/*
 * Verifies if Transaction passed as an argument to the function is already 
 * aborted or its g_valid flag is set to FALSE implying that Transaction
 * will be aborted soon.
 * */
bool PKTO::isAborted(GTransaction* gtrans)
{
	if(gtrans != NULL) {
		if(gtrans->g_valid == FALSE || gtrans->g_state == ABORT) {
			return TRUE;
		} else
			return FALSE;
	}
}

/*
 * Method used to release all the locks acquired by the transaction on any,
 * transaction objects or other transactions. 
 * */
void PKTO::unlockAll(LTransaction *ltrans)
{
	/*Check if the transaction has attain lock on any other transaction,
		if yes then release those locks*/
	if(ltrans->trans_locked->size() != ZERO) {
		list<GTransaction*>::iterator itr = ltrans->trans_locked->begin();
		while(itr != ltrans->trans_locked->end())
			{
				(*itr)->g_lock->unlock();
				itr++;
			}
	}
		
	/*Check if the transaction has attain lock on any of the transaction objects,
			if yes then realse all those locks too.*/
	if(ltrans->tobjs_locked->size() != ZERO) {
		list<long int>::iterator iter = ltrans->tobjs_locked->begin();
		while(iter != ltrans->tobjs_locked->end())
			{
			tobjs->at(*iter).tobj_lock->unlock();
			iter++;
		}
	}
	//clear the transaction and transactions object's lists
	ltrans->trans_locked->clear();
	ltrans->tobjs_locked->clear();
}

/************************ PKTO::PUBLIC METHODS ***********************/
/*
 * Invoked by a thread to start a new transaction. Thread can pass a parameter 'its'
 * which is the initial timestamp when this transaction was invoked for the 
 * first time. If this is the first invocation then 'its' is NIL.
 * */
LTransaction* PKTO::tbegin(long int its) {
	LTransaction *trans = new LTransaction;
	trans->id = g_tCntr.fetch_add(ONE);
			
	// If this is the first invocation		
	if(its == NIL)	{
			trans->g_its  = trans->g_cts = trans->id;
	} 
	// Invocation by an aborted transaction
	else {
		trans->g_its = its;
		trans->g_cts = trans->id;						
	}
	trans->g_state = LIVE;
	trans->g_valid = TRUE;
	trans->comTime = INFINITE;
	
	return trans;
}
	
/*
 * Invoked by a transaction T i to read tobj x.
 * ltrans - local transaction object
 * tobj_id - transaction object id
 * transaction_status - denotes status of the read transaction(OK/ABORTED)
 * 
 * */
bool PKTO::stmRead(LTransaction* ltrans, TobIdValPair* tobj_id_val_pair)														
{	
	/*To check whether transaction object with tobj_id 
	  is present in the writer's set of the transaction*/	
	if(find_set(ltrans->write_set, tobj_id_val_pair) == TRUE) {
		return OK;
	} 
	
	/*To check whether transaction object with tobj_id 
	  is present in the reader's set of the transaction*/
	if(find_set(ltrans->read_set,tobj_id_val_pair) == TRUE) {
		return OK;
	}
	
		//Global transaction instance from local transaction
		GTransaction *gtrans = ltrans;
		
		//Attain lock on transaction object
	tobjs->at(tobj_id_val_pair->id).tobj_lock->lock();
	ltrans->tobjs_locked->push_back(tobj_id_val_pair->id);
	//Attain lock on current transaction
	ltrans->g_lock->lock();
	ltrans->trans_locked->push_back(gtrans);
	
	//Abort the transaction is transaction's valid value is FALSE
	if(ltrans->g_valid == FALSE) {
		if(stmAbort(ltrans) == OK) {
			return ABORTED;
		}
	}
	
	//Find the largest wts Version less than g_wts of the transaction
	Version *curVer;
	curVer = findLTS_STL(ltrans->g_cts,tobj_id_val_pair->id);
	if(curVer == NULL) {
		if(stmAbort(ltrans) == OK) {
			return ABORTED;
		}
	}
	
	//Add the transaction object id and value pair to the reader's list	
	tobj_id_val_pair->val = curVer->val;
	ltrans->read_set->push_back(*tobj_id_val_pair);
	
	//Add transaction to current version reader's list
	insertAndSortRL(curVer->rl,gtrans);	
	
	//Unlock the transaction and unlock the transaction object
	unlockAll(ltrans);
	
	//return OK
	return OK;
}	

/*
 * A Transaction T writes into its local memory - 'write_set'
 * ltrans - local transaction object
 * tobj_id - transaction object id
 * val - value to be updated
 * transaction_status - denotes status of the read transaction(SUCCESS/ABOTED)
 * */
bool PKTO::stmWrite(LTransaction* ltrans, TobIdValPair* tobj_id_val_pair)
{
	//Flag to check weather the new transaction <id,val> pair is inserted or not
	bool flag = FALSE;
	
	//if write set of the transaction is empty insert the T<id,val> pair
	if(ltrans->write_set->size() == ZERO) {
		ltrans->write_set->push_back(*tobj_id_val_pair);
		return OK;
	} 
	//insert the T<id,val> pair and maintain the sorted order of ids of the transaction objects <id,val> pairs
	vector<TobIdValPair>::iterator it;
	for(it=ltrans->write_set->begin();it < ltrans->write_set->end();it++)
	{
		if((*it).id == tobj_id_val_pair->id) {
			it = ltrans->write_set->erase(it);
			flag = TRUE;
			break;
		} else if((*it).id > tobj_id_val_pair->id) {
			flag = TRUE;
			break;
		}
	}
	/*If the transaction object id is the lartgest amongst the transaction 
	 objects already present in the writer's set of the transaction, insert the TobjIdValPair in the last.*/
	if(flag == FALSE) {
		ltrans->write_set->push_back(*tobj_id_val_pair);
	} else {
		ltrans->write_set->insert(it,*tobj_id_val_pair);
	}
	return OK;
}

/*
 * Returns OK on commit else return ABORTED.
 * Method try to commit all the write operation stored in the, 
 * write set of the transaction.
 * */
bool PKTO::stmTryCommit(LTransaction* ltrans)
{
	
	list<long int> prevVL,nextVL;
	list<GTransaction*> *allRL = new list<GTransaction*>;
	//list<GTransaction*> *smallRL = new list<GTransaction*>;
	list<GTransaction*> *largeRL = new list<GTransaction*>;
	list<GTransaction*> *abortRL = new list<GTransaction*>;
	GTransaction *gtrans = ltrans;
	GTransaction *gtran_iterator;
	list<GTransaction*>::iterator gtran_list_iterator;
	long int objId;
	list<long int>::iterator ver_iterator;
	
	//Optimization check for validaity of the transaction	
	ltrans->g_lock->lock();
	ltrans->trans_locked->push_back(gtrans);
	if(ltrans->g_valid == FALSE) {
		if(stmAbort(ltrans) == OK) {
			return ABORTED;
		}
	}
	ltrans->trans_locked->clear();
	ltrans->g_lock->unlock();
	
	
	//lock all transaction objects:x belongs to write_set of the transaction in pre-defined order.
	for(int i = ZERO;i<ltrans->write_set->size();i++) {
		objId = ltrans->write_set->at(i).id;
		
		tobjs->at(objId).tobj_lock->lock();
		ltrans->tobjs_locked->push_back(objId);
		
		//Find the Version with largest wts value less than g_wts of the transaction
		Version *prevVer;
		prevVer = findLTS_STL(ltrans->g_cts,objId);
		//If no such version exists, abort the transaction and return ABORTED.
		if(prevVer == NULL) {
			ltrans->g_lock->lock();
			ltrans->trans_locked->push_back(gtrans);
			if(stmAbort(ltrans) == OK) {
				return ABORTED;	
			}
		}
		
		// Store the read-list of the previous version in allRL
		gtran_list_iterator = prevVer->rl->begin();
		while(gtran_list_iterator != prevVer->rl->end())
		{
			gtran_iterator = *gtran_list_iterator;
			insertAndSortRL(allRL,gtran_iterator);
			gtran_list_iterator++;
		}		
	}
	
	/*getLar: obtain the list of reading transactions of the previous version 
			whose g_wts is GREATER THAN g_wts of current transaction */
		list<GTransaction*> *preVerRL_GT;
		preVerRL_GT = getLar(ltrans->g_cts,allRL);
		if(preVerRL_GT != NULL) {
			gtran_list_iterator = preVerRL_GT->begin();
			while(gtran_list_iterator != preVerRL_GT->end())
			{
				gtran_iterator = *gtran_list_iterator;
				insertAndSortRL(largeRL,gtran_iterator);
				gtran_list_iterator++;
			}
		}
		
	//add current transaction and sort
	insertAndSortRL(largeRL,gtrans);
		
	//lock all the transactions of the largeRL list
	gtran_list_iterator = largeRL->begin();
	while(gtran_list_iterator != largeRL->end())
    {
		gtran_iterator = *gtran_list_iterator;
		gtran_iterator->g_lock->lock();		
		ltrans->trans_locked->push_back(gtran_iterator);
		gtran_list_iterator++;
	}
	
	//verify g_valid; if false then abort the transaction	
	if(ltrans->g_valid == FALSE) {
		if(stmAbort(ltrans) == OK) {
			return ABORTED;
		}
	}
	
	gtran_list_iterator = largeRL->begin();
	//transaction Tk among all the transactions in largeRL, either current transaction or Tk has to be aborted
	while(gtran_list_iterator != largeRL->end()) {
		gtran_iterator = *gtran_list_iterator;
		if(ltrans->g_cts == gtran_iterator->g_cts)
		{
			gtran_list_iterator++;
			continue;
		}
		if(isAborted(gtran_iterator)) {
			// Transaction T can be ignored since it is already aborted or about to be aborted
			gtran_list_iterator++;
			continue;
		}
		if((ltrans->g_its < gtran_iterator->g_its) && (gtran_iterator->g_state == LIVE)) {
			// if transaction has lower priority and is not yet committed. So it needs to be aborted
			insertAndSortRL(abortRL,gtran_iterator);
		} else {
			// Transaction has to be aborted
			if(stmAbort(ltrans) == OK) {
				return ABORTED;
			}
		}
		gtran_list_iterator++;
	}
	
	// Abort all the transactions in abortRL since current transaction can’t abort
	gtran_list_iterator = abortRL->begin();
	while(gtran_list_iterator != abortRL->end())
	{
		gtran_iterator = *gtran_list_iterator;
		if(gtran_iterator->g_state == LIVE) {
			gtran_iterator->g_valid = FALSE;
		}
		gtran_list_iterator++;
	}
	
	// Having completed all the checks, current transaction can be committed	
	for(int i = ZERO;i<ltrans->write_set->size();i++) {					
		Version *newVer = new Version;
		newVer->cts = ltrans->g_cts;
		newVer->val = ltrans->write_set->at(i).val;
				
		//method invoked to add Version to the transaction object's version list
		insertAndSortVL(newVer, ltrans->write_set->at(i).id);
	}
	
	//change the state of the transaction to COMMIT
	ltrans->g_state = COMMIT;
	
	//unlock all the variables
	unlockAll(ltrans);

	//return OK
	return OK;
}

/*
 * Invoked by various STM methods to abort transaction 'trans' passed as an 
 * argument to the function. It returns A;
 * */
bool PKTO::stmAbort(LTransaction* ltrans)
{
	if(ltrans != NULL) {
		//set the transaction's valid value as false and state as abort
		ltrans->g_valid = FALSE;
		ltrans->g_state = ABORT;
		//unlock all the variables
		unlockAll(ltrans);
		//Return OK status		
		return OK;
	}
	return ABORTED;
}
