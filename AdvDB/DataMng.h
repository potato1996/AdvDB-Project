/**
 * Author: Shiyao Lei (sl6569@nyu.edu)
 * Date: 2018-12-06
 * Description: Data manager is used to manage the data site. It can perform read, read only(for multi-version control),
 * write, commit, abort operations dispatched by transaction manager. It also perform recovery when site fails.
 *
**/
#pragma once

#include"Common.h"
#include<map>
#include<unordered_map>
#include<unordered_set>
#include<list>

class DataMng {
public:
    //------------- Basic stuffs goes here -----------------------
    siteid_t _site_id;
    bool _is_up;
    timestamp_t _last_up_time;

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

    //Before the TM trying to read or write an item, it should get the locks first

    // Get read lock: (S or X)
    // 1. Return true if it is safe to read
    // 2. Return false if:
    //    a. This item is not readable due to recovery (only when it is a replicated item)
    //    b. There's a lock conflict - side effect: the transaction will be scheduled into 
    //       lock waiting queue
    bool GetReadLock(transid_t trans_id, itemid_t item_id);

    // Get write lock (X)
    // 1. Return true if lock granted
    // 2. Return false if there's a lock conflict - side effect: the transaction will be 
    //    scheduled in to lock waiting queue
    bool GetWriteLock(transid_t trans_id, itemid_t item_id);


    // For the read operations, the result will be send back via TM's listener

    // Read operation: R1(X)
    // Return false if this item is not readable due to recovery 
    //    (only when it is a replicated item)
    // Ret: If we are allowed to read this item on this site
    bool Read(op_t op);

    // Read - Only transactions use multiversion concurrency control
    // Return false if this item is not readable due to recovery 
    //    (only when it is a replicated item)
    // Ret: If we are allowed to read this item on this site
    bool Ronly(op_t op, timestamp_t ts);

    // Write operation: W1(X, V)
    void Write(op_t op);

    // Commit a transaction (the caller should ensure that it is safe to commit)
    void Commit(transid_t trans_id, timestamp_t commit_time);

    // This is used by TM to retrive waiting graph from the DMs.
    // Which will be ultimately used for deadlock detection
    std::unordered_map<siteid_t, std::unordered_set<siteid_t>> GetWaitingGraph();

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
        int value;
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

    struct lock_queue_item_t {
        lock_type_t lock_type;
        transid_t trans_id;

        lock_queue_item_t() {
            lock_type = NONE;
            trans_id = -1;
        }

        lock_queue_item_t(transid_t _t, lock_type_t _l) {
            lock_type = _l;
            trans_id = _t;
        }

        bool operator==(lock_queue_item_t _other) {
            return trans_id == _other.trans_id && lock_type == _other.lock_type;
        }
    };

    // The lock structure on each data item
    struct lock_table_item_t {
        lock_type_t lock_type;
        std::unordered_set<transid_t> trans_holding;
        std::list<lock_queue_item_t> lock_queue;

        lock_table_item_t() {
            lock_type = NONE;
        }

        bool check_exist(lock_queue_item_t _lhs) {
            for (auto _rhs : lock_queue) {
                if (_lhs == _rhs) {
                    return true;
                }
            }
            return false;
        }
    };

    std::unordered_map<itemid_t, lock_table_item_t> _lock_table;

    //------------- Active Transaction Table ---------------------
    struct trans_table_item {
        std::unordered_set<itemid_t> modified_item;

        // std::unordered_set<itemid_t> locks_holding;
        // std::unordered_set<itemid_t> locks_waiting;
        trans_table_item() {}
    };

    std::unordered_map<transid_t, trans_table_item> _trans_table;


    //------------- Internal helper functions ---------------------
    // Return true if it is safe to grant lock. false otherwise
    // bool check_conflict(itemid_t item_id, transid_t trans_id, op_type_t op_type);

    // Try to resolve lock queue items in the lock table
    // (will do recursivly until no more new locks can be granted)
    void try_resolve_lock_table();

    // I believe that each transaction should only have one item in the lock queue
    // - So let's check it
    bool check_lock_queue();

    // check if two lock queue item are conflict
    // Return true if no conflict, false otherwise
    bool check_item_conflict(lock_queue_item_t _lhs, lock_queue_item_t _rhs);

    // check if we already hold the lock for this item without any update to the lock table
    bool check_already_hold(itemid_t item_id, lock_queue_item_t _rhs);

    // check if a lock queue item is conflict with current lock holding
    // Return true if no conflict, false otherwise
    bool check_holding_conflict(itemid_t item_id, lock_queue_item_t _rhs);

    // check if a lock queue item is conflict with any lock item currently in the lock queue
    // Return true if no conflict, false otherwise
    bool check_queued_conflict(itemid_t item_id, lock_queue_item_t _rhs);
};