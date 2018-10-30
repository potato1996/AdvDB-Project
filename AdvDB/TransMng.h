#pragma once
#include"Common.h"
#include<unordered_map>
class TransMng {
public:
	TransMng();

	// The main simulation loop
	void Simulate();

	// The response of a read operation
	void ReadResponse(trans_dscr_t trans, itemid_t item_id, int value);


private:
	timestamp_t _now;

	// Transaction Table
	std::unordered_map<transid_t, trans_dscr_t> _trans_table;

	//--------------------tester cause events----------------------
	void Fail(siteid_t site_id);

	void Recover(siteid_t site_id);

	void DumpAll();

	void Dump(siteid_t site_id);

	//-----------------transaction execution events----------------
	void Begin(trans_dscr_t trans);

	void Finish(trans_dscr_t trans);

	void Abort(trans_dscr_t trans);

	void Read(trans_dscr_t trans, itemid_t item_id);

	void Write(trans_dscr_t trans, itemid_t item_id, int value);
};