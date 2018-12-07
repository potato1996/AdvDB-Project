#include "DataMng.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <queue>

#include "Common.h"
#include "TransMng.h"

extern TransMng* TM;

namespace {
    bool is_replicated(itemid_t item_id) {
        return item_id % 2 == 0;
    }

    void err_invalid_case() {
        std::cout << "ERROR: Invalid Switch Case\n";
        std::exit(-1);
    };

    void err_inconsist() {
        std::cout << "ERROR: Internal state inconsist\n";
        std::exit(-1);
    };
} // helper functions

DataMng::DataMng(siteid_t site_id) {
    // basic stuff
    _site_id = site_id;
    _is_up = true;
    _last_up_time = -1;

    // initialize the data items
    for (itemid_t item_id = 1; item_id <= ITEM_COUNT; item_id++) {
        if ((item_id % 2 == 0) || (1 + (item_id % 10) == site_id)) {
            // commit time = -1 means initial values
            _disk[item_id].push_back(disk_item(item_id * 10, -1));

            // since we have small number of data items, we can cache all of them in memory
            _memory[item_id] = mem_item(item_id * 10);
            _readable[item_id] = true;

        }
    }
}

void
DataMng::Abort(transid_t trans_id) {
    const auto& trans_info = _trans_table[trans_id];

    // clean up the locks
    for (auto &p : _lock_table) {
        lock_table_item_t &lock_item = p.second;
        if (lock_item.trans_holding.count(trans_id)) {
            // clean up the lock table
            lock_item.trans_holding.erase(trans_id);

            // clean up the lock queue
            lock_item.lock_queue.remove_if([trans_id](const lock_queue_item_t& item) {
                return item.trans_id == trans_id;
            });

            // free up the lock
            if (lock_item.trans_holding.empty()) {
                lock_item.lock_type = NONE;
            }
        }
    }

    // recover the modifed data
    for (itemid_t item_id : _trans_table[trans_id].modified_item) {
        _memory[item_id].value = _disk[item_id].front().value;
    }

    // clean up trans table
    _trans_table.erase(trans_id);

    // now we freed up some locks, hopefully we can execute some commands
    try_resolve_lock_table();
}

void
DataMng::Dump() {
    std::cout << "site " << _site_id << " - ";
    for (const auto& p : _disk) {
        std::cout << "x" << p.first << ": " << p.second.front().value << ", ";
    }
    std::cout << std::endl;
}

void
DataMng::DumpItem(itemid_t item_id) {
    std::cout << "site " << _site_id << " - ";
    std::cout << "x" << item_id << ": " << _disk[item_id].front().value << std::endl;
}

void
DataMng::Fail() {
    // simply clean up the memory related stuff
    _is_up = false;
    _memory.clear();
    _readable.clear();
    _lock_table.clear();
    _trans_table.clear();
}

void
DataMng::Recover(timestamp_t _ts) {
    _is_up = true;
    _last_up_time = _ts;

    // we don't allow to read replicated data until we COMMIT a write on it
    for (const auto& p : _disk) {
        itemid_t  item_id = p.first;
        disk_item latest_val = p.second.front();
        _memory[item_id] = mem_item(latest_val.value);
        _readable[item_id] = !is_replicated(item_id);
    }
}


bool 
DataMng::GetReadLock(transid_t trans_id, itemid_t item_id) {

    if (!_readable[item_id]) {
        return false;
    }

    lock_queue_item_t new_queue_item(trans_id, S);
    lock_table_item_t& lock_item = _lock_table[item_id];

    if (check_already_hold(item_id, new_queue_item) ||
        (check_holding_conflict(item_id, new_queue_item) &&
        check_queued_conflict(item_id, new_queue_item))) {
        // update the lock table
        if (lock_item.lock_type == NONE) {
            lock_item.lock_type = S;
        }
        lock_item.trans_holding.insert(trans_id);

        // grant lock
        return true;
    }
    else {
        // append this operation to the end of the lock queue
        if (!lock_item.check_exist(new_queue_item)) {
            lock_item.lock_queue.push_back(new_queue_item);
        }

        // update the transaction table
        // _trans_table[trans_id].locks_waiting.insert(item_id);
        return false;
    }
    err_invalid_case();
    return false;
}

bool
DataMng::Read(op_t op) {
    itemid_t  item_id = op.param.r_param.item_id;
    transid_t trans_id = op.trans_id;

    if (!_readable[item_id]) {
        return false;
    }

    if (_lock_table[item_id].trans_holding.count(trans_id)) {
        // execute the operation
        int value = _memory[item_id].value;

        // send the result back to TM
        TM->ReceiveReadResponse(op, _site_id, value);
        return true;
    }
    else {
        std::cout << "ERROR: Unsafe to read\n";
    }

    return false;
}

bool
DataMng::Ronly(op_t op, timestamp_t ts) {
    itemid_t  item_id = op.param.r_param.item_id;
    transid_t trans_id = op.trans_id;

    // go through the disk, find the latest commit before this ts
    const auto& value_list = _disk[item_id];
    for (auto it = value_list.begin(); it != value_list.end(); it++) {
        if (it->commit_time <= ts) {
            // for r-only transactions, we have a different logic of "non-readable":
            // We need the found commit to be after latest recover time
            if (is_replicated(item_id) && it->commit_time < _last_up_time) {
                return false;
            }
            else {
                TM->ReceiveReadResponse(op, _site_id, it->value);
                return true;
            }
        }
    }

    err_inconsist();
    return false;
}

bool 
DataMng::GetWriteLock(transid_t trans_id, itemid_t item_id) {

    lock_queue_item_t new_queue_item(trans_id, X);
    lock_table_item_t& lock_item = _lock_table[item_id];

    if (check_already_hold(item_id, new_queue_item) ||
        (check_holding_conflict(item_id, new_queue_item) &&
        check_queued_conflict(item_id, new_queue_item))) {

        // upgrade the lock type
        if (lock_item.lock_type == S || lock_item.lock_type == NONE) {
            lock_item.lock_type = X;
        }
        lock_item.trans_holding.insert(trans_id);

        // update the transaction table
        // _trans_table[trans_id].locks_holding.insert(item_id);

        // Grant lock to TM
        return true;
    }
    else {

        // append this operation to the end of the lock queue
        if (!lock_item.check_exist(new_queue_item)) {
            lock_item.lock_queue.push_back(new_queue_item);
        }

        // update the transaction table
        // _trans_table[trans_id].locks_waiting.insert(item_id);

        return false;
    }
    err_inconsist();
    return false;
}

void
DataMng::Write(op_t op) {
    itemid_t  item_id = op.param.w_param.item_id;
    int       write_val = op.param.w_param.value;
    transid_t trans_id = op.trans_id;

    if (check_already_hold(item_id, lock_queue_item_t(trans_id, X))) {

        // update the transaction table
        _trans_table[trans_id].modified_item.insert(item_id);

        // execute the operation
        _memory[item_id].value = write_val;

        TM->ReceiveWriteResponse(op, _site_id);
    }
    else {
        std::cout << "ERROR: Unsafe to write\n";
    }
}

void
DataMng::Commit(transid_t trans_id, timestamp_t commit_time) {
    auto err_not_safe_commit = []() {
        std::cout << "Debug Info: Items in lock queue at commit time\n";
    };

    // clean up the locks
    for (auto &p : _lock_table) {
        lock_table_item_t &lock_item = p.second;
        if (lock_item.trans_holding.count(trans_id)) {
            // clean up the lock table
            lock_item.trans_holding.erase(trans_id);

            // clean up the lock queue
            size_t ori_size = lock_item.lock_queue.size();
            lock_item.lock_queue.remove_if([trans_id](const lock_queue_item_t& item) {
                return item.trans_id == trans_id;
            });
            size_t new_size = lock_item.lock_queue.size();
            if (ori_size != new_size) {
                err_not_safe_commit();
            }

            // free up the lock
            if (lock_item.trans_holding.empty()) {
                lock_item.lock_type = NONE;
            }
        }
    }

    // write everything changed back to disk
    for (itemid_t item_id : _trans_table[trans_id].modified_item) {
        int value = _memory[item_id].value;
        _disk[item_id].push_front(disk_item(value, commit_time));

        // now we allow to read this value
        _readable[item_id] = true;
    }

    // clean up transaction table
    _trans_table.erase(trans_id);

    // now, since we have committed a transaction, hopefully we can finish some queued operations
    try_resolve_lock_table();
}

void
DataMng::try_resolve_lock_table() {
    bool flag = true;
    while (flag) {
        flag = false;
        for (auto &p : _lock_table) {
            itemid_t          item_id = p.first;
            lock_table_item_t &lock_item = p.second;
            if (!lock_item.lock_queue.empty()) {
                // we peek at the next operation, and check if we are safe to execute it
                auto next_item = lock_item.lock_queue.front();

                switch (next_item.lock_type) {
                case S: {
                    transid_t next_trans_id = next_item.trans_id;
                    if (check_holding_conflict(item_id, next_item)) {
                        
                        // now we should be able to remove this lock waiting item
                        lock_item.lock_queue.pop_front();

                        // also grant the new lock here
                        if (lock_item.lock_type == NONE) {
                            lock_item.lock_type = S;
                        }
                        lock_item.trans_holding.insert(next_trans_id);
                        flag = true;
                    }
                    break;
                }
                case X: {
                    transid_t next_trans_id = next_item.trans_id;
                    if (check_holding_conflict(item_id, next_item)) {
                        
                        // now we should be able to remove this lock waiting item
                        lock_item.lock_queue.pop_front();
                        
                        // also grant the new lock here
                        if (lock_item.lock_type == S || lock_item.lock_type == NONE) {
                            lock_item.lock_type = X;
                        }
                        lock_item.trans_holding.insert(next_trans_id);
                        flag = true;
                    }
                    break;
                }
                default:
                    err_invalid_case();
                }
            }
        }
    }
}


std::unordered_map<siteid_t, std::unordered_set<siteid_t>>
DataMng::GetWaitingGraph() {
    std::unordered_map<siteid_t, std::unordered_set<siteid_t>> graph;

    // now go through the locks table
    for (auto &p : _lock_table) {
        lock_table_item_t& lock_item = p.second;
        if (lock_item.lock_type == NONE || lock_item.lock_queue.size() == 0) {
            continue;
        }

        // 1. check if each item in the lock_queue is conflict with the now holding one
        for (lock_queue_item_t parent_item: lock_item.lock_queue) {
            if (!check_holding_conflict(p.first, parent_item)) {
                for (itemid_t child : lock_item.trans_holding) {
                    itemid_t parent = parent_item.trans_id;
                    if (parent != child) {
                        graph[parent].insert(child);
                    }
                }
            }
        }

        // 2. all the ops in the queue are waiting for the previous ones (if conflict)
        for (auto it_parent = lock_item.lock_queue.begin(); it_parent != lock_item.lock_queue.end(); it_parent++){
            for (auto it_child = lock_item.lock_queue.begin(); it_child != it_parent; it_child++) {
                if (check_item_conflict(*it_parent, *it_child)) {
                    itemid_t parent = it_parent->trans_id;
                    itemid_t child = it_child->trans_id;
                    if (parent != child) {
                        graph[parent].insert(child);
                    }
                }
            }
        }
    }

    return graph;
}


bool
DataMng::check_lock_queue() {
    for (auto &p : _lock_table) {
        std::unordered_set<transid_t> tmp;
        for (auto &item : p.second.lock_queue) {
            if (tmp.count(item.trans_id)) {
                return false;
            }
            tmp.insert(item.trans_id);
        }
    }
    std::cout << "Debug Info: same item occured twice in the same lock queue\n";
    return false;
}

bool
DataMng::check_item_conflict(lock_queue_item_t _lhs, lock_queue_item_t _rhs) {
    if (_lhs.trans_id == _rhs.trans_id) {
        return true;
    }
    if ((_lhs.lock_type == S) && (_rhs.lock_type == S)) {
        return true;
    }
    return false;
}

bool
DataMng::check_already_hold(itemid_t item_id, lock_queue_item_t _rhs) {
    const lock_table_item_t& lock_item = _lock_table[item_id];
    const transid_t trans_id = _rhs.trans_id;

    // first, we must hold it
    if (!lock_item.trans_holding.count(trans_id)) return false;

    // second, the lock type is match
    if (_rhs.lock_type == S) return true;
    else if (lock_item.lock_type == X)return true;
    else return false;
}

bool 
DataMng::check_holding_conflict(itemid_t item_id, lock_queue_item_t _rhs) {
    const lock_table_item_t& lock_item = _lock_table[item_id];
    const transid_t trans_id = _rhs.trans_id;
    switch (lock_item.lock_type) {
    case NONE:                                          return true;
    case S: {
        if (_rhs.lock_type == S)                        return true;
        else if (lock_item.trans_holding.size() == 1 &&
            lock_item.trans_holding.count(trans_id))    return true;
        else                                            return false;
    }
    case X: {
        if (lock_item.trans_holding.count(trans_id))    return true;
        else                                            return false;
    }
    default:
        err_invalid_case();
    }
    return false;
}

bool
DataMng::check_queued_conflict(itemid_t item_id, lock_queue_item_t _rhs) {
    const lock_table_item_t& lock_item = _lock_table[item_id];
    for (const auto& lock_queue_item : lock_item.lock_queue) {
        if (!check_item_conflict(_rhs, lock_queue_item)) {
            return false;
        }
    }
    return true;
}