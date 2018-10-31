#pragma once
#include"Common.h"
#include<unordered_map>
#include<unordered_set>
class TransMng {
public:
	TransMng();

	// The main simulation loop
	void Simulate();

	// The response of a read operation
	void ReadResponse(transid_t trans, itemid_t item_id, int value);


private:
	//------------- Basic stuffs goes here -----------------------
	timestamp_t _now;

	//------------- Active Transaction Table ---------------------
	struct trans_table_item {
		timestamp_t					 start_ts;
		bool						 is_ronly;
		bool					     will_abort;
		std::unordered_set<siteid_t> visited_sites;
	};
	std::unordered_map<transid_t, trans_table_item> _trans_table;
	
	// Queued Ops - recall that there could be no available sites
	std::list<op_t> _queued_ops;

	//--------------------tester cause events----------------------
	void Fail(siteid_t site_id);

	void Recover(siteid_t site_id);

	void DumpAll();

	void Dump(siteid_t site_id);

	//-----------------transaction execution events----------------
	void Begin(transid_t trans_id);

	void Finish(transid_t trans_id);

	void Abort(transid_t trans_id);

	void Read(transid_t trans_id, itemid_t item_id);

	void Write(transid_t trans_id, itemid_t item_id, int value);
};