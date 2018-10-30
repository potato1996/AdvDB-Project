#pragma once
#include"Common.h"
#include<unordered_map>
class TransMng {
public:
	TransMng();

	// The main simulation loop
	void Simulate();

	// The response of a read operation
	void ReadResponse(transid_t trans, itemid_t item_id, int value);


private:
	timestamp_t _now;

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