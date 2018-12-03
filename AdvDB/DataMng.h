#pragma once
#include"Common.h"
#include<map>
#include<unordered_map>
#include<unordered_set>
#include<list>
class DataMng {
public:
    //------------- Basic stuffs goes here -----------------------
    siteid_t     _site_id;
    bool         _is_up;
    timestamp_t  _last_up_time;

    // Follow the data initialization rules for the given site_id
    DataMng(siteid_t site_id);

    //--------------------tester cause events----------------------
    // Fail this site
    void Fail();

    // Recover this site
    void Recover(timestamp_t _ts);

    // Dump all of the disk values
    void Dump();

    // Dump one item
    void DumpItem(itemid_t item_id);

    //-----------------transaction execution events----------------
    // Abort an transaction
    void Abort(transid_t trans_id);

    // Read operation: R1(X)
    // 1. The result of a read operation may NOT return immediately
    //    it could be waiting on a X lock
    // 2. Return false if this item is not readable due to recovery 
    //    (only when it is a replicated item)
    // Ret: If we are allowed to read this item on this site
    bool Read(op_t op);

    // Read - Only transactions use multiversion concurrency control
    bool Ronly(op_t op, timestamp_t ts);

    // Write operation: W1(X)
    void Write(op_t op);

    // Check if all operation for the transaction has finished on this site
    bool CheckFinish(transid_t trans_id);

    // Commit a transaction (the caller should ensure that it is safe to commit)
    void Commit(transid_t trans_id, timestamp_t commit_time);

    // Detect deadlock, return -1 if there's really no deadlocks
    transid_t DetectDeadLock();

private:
    //------------- Storage goes here ----------------------------
    // For temporal storage(memory), it seems do not need a timestamp version
    struct mem_item {
        int value;
        mem_item() {}
        mem_item(int _value) {
            value = _value;
        }
    };
    std::unordered_map<itemid_t, mem_item> _memory;

    // reads will not be allowed at recovered sites until a committed write takes place
    std::unordered_map<itemid_t, bool> _readable;

    // For non-volatile storage(disk), we use multi-version control
    // Here I use a map because the dump function needs an order
    struct disk_item {
        int         value;
        timestamp_t commit_time;
        disk_item() {}
        disk_item(int _value, timestamp_t _ts) {
            value = _value;
            commit_time = _ts;
        }
    };
    std::map<itemid_t, std::list<disk_item>> _disk;

    //------------- Now begin the lock part ----------------------
    enum lock_type_t {
        NONE,
        S,
        X
    };

    // The lock structure on each data item
    struct lock_table_item {
        lock_type_t                      lock_type;
        std::unordered_set<transid_t>    trans_holding;
        std::list<op_t>                  queued_ops;
        lock_table_item() {
            lock_type = NONE;
        }
    };
    std::unordered_map<itemid_t, lock_table_item> _lock_table;

    //------------- Active Transaction Table ---------------------
    struct trans_table_item {
        timestamp_t                  start_ts;
        std::unordered_set<itemid_t> locks_holding;
        std::unordered_set<itemid_t> locks_waiting;
        trans_table_item() {}
        trans_table_item(timestamp_t _ts) {
            start_ts = _ts;
        }
    };
    std::unordered_map<transid_t, trans_table_item> _trans_table;


    //------------- Internal helper functions ---------------------
    // Return true if it is safe to execute. false otherwise
    bool check_conflict(itemid_t item_id, transid_t trans_id, op_type_t op_type);

    // Try to execute queued operations in the lock table
    // (will do recursivly until no more new ops can be executed)
    void try_execute();
};