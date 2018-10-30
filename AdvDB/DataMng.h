#pragma once
#include"Common.h"
#include<set>
#include<map>
#include<unordered_map>
#include<queue>
#include<list>
class DataMng {
public:
	// Follow the data initialization rules for the given site_id
	DataMng(siteid_t site_id);
	
	//--------------------tester cause events----------------------
	// Fail this site
	void Fail();

	// Recover this site
	void Recover();

	// Dump the disk values
	void Dump();

	//-----------------transaction execution events----------------
	// Abort an transaction
	void Abort(trans_dscr_t trans);

	// Read operation: R1(X)
	// 1. The result of a read operation may NOT return immediately
	//    it could be waiting on a X lock
	// 2. Return false if the site is not reachable (failed),
	//    or, this item is not readable due to recovery (only when it is a replicated item)
	// 3. There's also a special case for Read-Only transactions
	bool Read(trans_dscr_t trans, itemid_t item_id);

	// Write operation: W1(X)
	// return false if the site is not reachable (failed)
	bool Write(trans_dscr_t trans, itemid_t item_id, int value);

	// Indicate that a transaction has ended: end(T1)
	// return false if the site is not reachable (failed)
	bool Finish(trans_dscr_t trans, timestamp_t commit_time);

	// Detect deadlock, return -1 if there's really no deadlocks
	transid_t DetectDeadLock();
	
private:
	//------------- Basic stuffs goes here -----------------------
	siteid_t _site_id;
	bool     _is_up;

	//------------- Storage goes here ----------------------------
	// For temporal storage(memory), it seems do not need a timestamp version
	struct mem_item{
		int  value;
		bool is_readable;
	};
	std::unordered_map<itemid_t, mem_item> memory;

	// For non-volatile sotrage, we use multi-version control
	// Here I use a map because the dump function needs an order
	struct disk_item {
		int			value;
		timestamp_t commit_time;
	};
	std::map<itemid_t, std::list<disk_item>> disk;

	//------------- Now begin the lock part ----------------------
	enum lock_type_t {
		S,
		X
	};

	// The lock structure on each data item
	struct Lock {
		lock_type_t			lock_type;
		std::set<transid_t> now_holding;
		std::list<op_t>		queued_ops;
	};

	// Our "lock table"!
	std::unordered_map<itemid_t, Lock> _locks;
};