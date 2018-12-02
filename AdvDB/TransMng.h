#pragma once
#include"Common.h"
#include<unordered_map>
#include<unordered_set>
class TransMng {
public:
    TransMng();

    // The main simulation loop
    void Simulate(std::istream inputs);

    // The response of a read operation
    void ReceiveReadResponse(op_t op, int value);

    // The response of a write operation
    void ReceiveWriteResponse(op_t op);

    // DMs would use it to get the start time of transactions
    // Will be used to determine which to abort at cycle detection
    timestamp_t QueryTransStartTime(transid_t trans_id);

private:
    //------------- Basic stuffs goes here -----------------------
    timestamp_t _now;
    opid_t      _next_opid;

    //------------- Site Status ----------------------------------
    bool _site_status[SITE_COUNT];

    std::unordered_map<itemid_t, std::list<siteid_t>> _item_sites;

    //------------- Active Transaction Table ---------------------
    struct trans_table_item {
        timestamp_t                  start_ts;
        bool                         is_ronly;
        bool                         will_abort;
        bool                         waiting_commit;
        std::unordered_set<siteid_t> visited_sites;
        trans_table_item() {}
        trans_table_item(timestamp_t ts, bool ronly) {
            start_ts       = ts;
            is_ronly       = ronly;
            will_abort     = false;
            waiting_commit = false;
        }
    };
    std::unordered_map<transid_t, trans_table_item> _trans_table;
    
    // Queued Ops and Finished ops - recall that there could be no available sites
    std::list<op_t> _queued_ops;

    //--------------------tester cause events----------------------
    void Fail(siteid_t site_id);

    void Recover(siteid_t site_id);

    void DumpAll();

    void Dump(siteid_t site_id);

    //-----------------transaction execution events----------------
    void Begin(transid_t trans_id, bool is_ronly);

    void Finish(transid_t trans_id);

    void Abort(transid_t trans_id);

    void Read(transid_t trans_id, itemid_t item_id);

    void Write(transid_t trans_id, itemid_t item_id, int value);
};