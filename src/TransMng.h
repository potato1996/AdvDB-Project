/**
 * Author: Dayou Du (dd2645@nyu.edu)
 * Date: 2018-12-06
 * Description: Transaction manager translates read and write requests on variables to read and write request on copies.
 *
**/
#pragma once

#include"Common.h"
#include<unordered_map>
#include<unordered_set>
#include<list>

class TransMng {
public:
    TransMng();

    // The main simulation loop
    void Simulate(std::istream &inputs);

    // The response of a read operation
    void ReceiveReadResponse(op_t op, siteid_t site_id, int value);

    // The response of a write operation
    void ReceiveWriteResponse(op_t op, siteid_t site_id);

private:
    //------------- Basic stuffs goes here -----------------------
    timestamp_t _now;
    opid_t _next_opid;

    //------------- Site Status ----------------------------------
    // For simplicity we deal with the annoying 1-index here
    bool _site_status[SITE_COUNT + 1];

    std::unordered_map<itemid_t, std::list<siteid_t>> _item_sites;

    //------------- Active Transaction Table ---------------------
    struct trans_table_item {
        timestamp_t start_ts;
        bool is_ronly;
        bool will_abort;
        bool waiting_commit;
        std::unordered_set<siteid_t> visited_sites;

        trans_table_item() {}

        trans_table_item(timestamp_t ts, bool ronly) {
            start_ts = ts;
            is_ronly = ronly;
            will_abort = false;
        }
    };

    std::unordered_map<transid_t, trans_table_item> _trans_table;

    // Queued Ops and Finished ops - recall that there could be no available sites
    std::list<op_t> _queued_ops;

    //--------------------tester cause events----------------------
    void Fail(siteid_t site_id);

    void Recover(siteid_t site_id);

    void DumpAll();

    void DumpSite(siteid_t site_id);

    void DumpItem(itemid_t item_id);

    //-----------------transaction execution events----------------
    bool DetectDeadLock();

    void TryExecuteQueue();

    void ExecuteCommand(std::string line);

    void Begin(transid_t trans_id, bool is_ronly);

    void Finish(transid_t trans_id);

    void Abort(transid_t trans_id);

    bool Read(op_t op);

    bool Ronly(op_t op);

    bool Write(op_t op);
};
