#include "DataMng.h"

#include <cstdlib>
#include <iostream>

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
    _site_id      = site_id;
    _is_up        = true;
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
    if (!_trans_table.count(trans_id))return;
    const auto& trans_info = _trans_table[trans_id];
    
    // clean up the locks that now holding
    for (const itemid_t item_id : trans_info.locks_holding) {
        // clean up lock table
        lock_table_item &lock_item = _lock_table[item_id];

        // Ensure that we really hold this lock - i.e. 2pc holds
        if (!lock_item.trans_holding.count(trans_id)) {
            err_inconsist();
        }

        // clean up lock table
        lock_item.trans_holding.erase(trans_id);

        // free up the lock
        if (lock_item.trans_holding.empty()) {
            lock_item.lock_type = NONE;
        }
    }

    // clean up the ops that now waiting
    for (const itemid_t item_id : trans_info.locks_waiting) {
        // clean up lock table
        lock_table_item &lock_item = _lock_table[item_id];

        // remove all the commands by trans_id
        lock_item.queued_ops.remove_if([trans_id](const op_t& op) {
            return op.trans_id == trans_id;
        });
    }

    // clean up trans table
    _trans_table.erase(trans_id);

    // now we freed up some locks, hopefully we can execute some commands
    try_execute();
}

void
DataMng::Dump() {
    std::cout << "site " << _site_id << " - ";
    for (const auto& p : _disk) {
        std::cout << "x" << p.first << ": " << p.second.front().value <<", ";
    }
    std::cout << std::endl;
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
    _is_up        = true;
    _last_up_time = _ts;

    // we don't allow to read replicated data until we COMMIT a write on it
    for (const auto& p : _disk) {
        itemid_t  item_id    = p.first;
        disk_item latest_val = p.second.front();
        _memory[item_id]     = mem_item(latest_val.value);
        _readable[item_id]   = !is_replicated(item_id);
    }
}

bool
DataMng::Read(op_t op) {
    itemid_t  item_id  = op.param.r_param.item_id;
    transid_t trans_id = op.trans_id;
    
    if (!_readable[item_id]) {
        return false;
    }

    // initialize transaction table item, if this is the first time we met it.
    if (!_trans_table.count(trans_id)) {
        _trans_table[trans_id] = trans_table_item(TM->QueryTransStartTime(trans_id));
    }

    if (check_conflict(item_id, trans_id, OP_READ)) {
        // update the lock table
        lock_table_item& lock_item = _lock_table[item_id];
        if (lock_item.lock_type == NONE) {
            lock_item.lock_type = S;
        }
        lock_item.trans_holding.insert(trans_id);

        // update the transaction table
        _trans_table[trans_id].locks_holding.insert(item_id);

        // execute the operation
        int value = _memory[item_id].value;

        // send the result back to TM
        TM->ReceiveReadResponse(op, value);
    }
    else {
        // append this operation to the end of the lock queue
        _lock_table[item_id].queued_ops.push_back(op);

        // update the transaction table
        _trans_table[trans_id].locks_waiting.insert(item_id);
    }

    return true;
}

bool
DataMng::Ronly(op_t op, timestamp_t ts) {
    itemid_t  item_id  = op.param.r_param.item_id;
    transid_t trans_id = op.trans_id;

    // go through the disk, find the latest commit before this ts
    const auto& value_list = _disk[item_id];
    for (auto it = value_list.begin(); it != value_list.end(); it++) {
        if (it->commit_time <= ts) {
            // for r-only transactions, we have a different logic of "non-readable":
            // We need the found commit to be after latest recover time
            if (it->commit_time < _last_up_time) {
                return false;
            }
            else {
                TM->ReceiveReadResponse(op, it->value);
                return true;
            }
        }
    }

    err_inconsist();
    return false;
}

void
DataMng::Write(op_t op) {
    itemid_t  item_id   = op.param.w_param.item_id;
    int       write_val = op.param.w_param.value;
    transid_t trans_id  = op.trans_id;

    // initialize transaction table item, if this is the first time we met it.
    if (!_trans_table.count(trans_id)) {
        _trans_table[trans_id] = trans_table_item(TM->QueryTransStartTime(trans_id));
    }

    if (check_conflict(item_id, trans_id, OP_WRITE)) {
        // upgrade the lock type
        lock_table_item& lock_item = _lock_table[item_id];
        if (lock_item.lock_type == S || lock_item.lock_type == NONE) {
            lock_item.lock_type = X;
        }
        lock_item.trans_holding.insert(trans_id);

        // update the transaction table
        _trans_table[trans_id].locks_holding.insert(item_id);

        // execute the operation
        _memory[item_id].value = write_val;

        // Report to TM that we have finished a write
        TM->ReceiveWriteResponse(op);
    }
    else {
        // append this operation to the end of the lock queue
        _lock_table[item_id].queued_ops.push_back(op);

        // update the transaction table
        _trans_table[trans_id].locks_waiting.insert(item_id);
    }
}

void
DataMng::Commit(transid_t trans_id, timestamp_t commit_time) {
    auto err_not_safe_commit = [](){
        std::cout << "ERROR: NOT safe to commit\n";
    };

    if (!_trans_table.count(trans_id)) {
        return;
    }

    if (!CheckFinish(trans_id)) {
        err_not_safe_commit();
    }

    // clean up the locks and write everything back to disk
    for (itemid_t item_id : _trans_table[trans_id].locks_holding) {
        lock_table_item &lock_item = _lock_table[item_id];
        
        // Ensure that we really hold this lock - i.e. 2pc holds
        if (!lock_item.trans_holding.count(trans_id)) {
            err_inconsist();
        }

        // clean up lock table
        lock_item.trans_holding.erase(trans_id);

        // free up the lock
        if (lock_item.trans_holding.empty()) {
            lock_item.lock_type = NONE;
        }

        // write values back to disk
        int value = _memory[item_id].value;
        _disk[item_id].push_front(disk_item(value, commit_time));

        // now we allow to read this value
        _readable[item_id] = true;
    }

    // clean up transaction table
    _trans_table.erase(trans_id);

    // now, since we have committed a transaction, hopefully we can finish some queued operations
    try_execute();
}

void
DataMng::try_execute() {
    bool flag = true;
    while (flag) {
        flag = false;
        for (auto &p : _lock_table) {
            itemid_t        item_id = p.first;
            lock_table_item &lock_item = p.second;
            if (!lock_item.queued_ops.empty()) {
                // we peek at the next operation, and check if we are safe to execute it
                op_t next_op = lock_item.queued_ops.front();

                switch (next_op.op_type) {
                case OP_READ: {
                    itemid_t  next_item_id = next_op.param.r_param.item_id;
                    transid_t next_trans_id = next_op.trans_id;
                    if ((lock_item.lock_type == S) ||
                        (check_conflict(next_item_id, next_trans_id, OP_READ))) {
                        // now we should be able to execute this op
                        lock_item.queued_ops.pop_front();
                        Read(next_op);
                        flag = true;
                    }
                    break;
                }
                case OP_WRITE: {
                    itemid_t  next_item_id = next_op.param.w_param.item_id;
                    int       write_val = next_op.param.w_param.value;
                    transid_t next_trans_id = next_op.trans_id;
                    if (check_conflict(next_item_id, next_trans_id, OP_WRITE)) {
                        // now we should be able to execute this op
                        lock_item.queued_ops.pop_front();
                        Write(next_op);
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

bool
DataMng::CheckFinish(transid_t trans_id) {
    if (!_trans_table.count(trans_id)) {
        return true;
    }
    return _trans_table[trans_id].locks_waiting.empty();
}

transid_t
DataMng::DetectDeadLock() {
    queue<trasid_t> currentZeroIn;
    map<transid_t, int> indegree;
    for (auto p: _trans_table) {
        indegree[p.first] = 0;
    }
    for (auto p : _trans_table) {
        transid_t  trans_id = p.first;
        for (itemid_t item_id : trans_table[trans_id].locks_waiting) {
            for (transid_t next_id : _lock_table[item_id].trans_holding) {
                if (next_id != trans_id) {
                    indegree[next_id] ++;
                }
            }
        }
    }
    for (auto p : indegree) {
        if (p.second == 0)
            currentZeroIn.push(p.first);
    }
    while (!currentZeroIn.isEmpty()) {
        transid_t id = currentZeroIn.pop();
        for (itemid_t item_id : trans_table[id].locks_waiting) {
            for (transid_t next_id : _lock_table[item_id].trans_holding) {
                if (--indegree[next_id] == 0) {
                    currentZeroIn.push(next_id);
                }
            }
        }
    }
    int oldest = -1;
    int oldest_transid = -1;
    for (auto p : indegree) {
        if (p.second != 0) {
            if (oldest < _trans_table[p.first].start_ts) {
                oldest = _trans_table[p.first].start_ts;
                oldest_transid = p.first;
            }
        }
    }
    return oldest_transid;
}


bool 
DataMng::check_conflict(itemid_t item_id, transid_t trans_id, op_type_t op_type) {
    const lock_table_item& lock_item = _lock_table[item_id];

    // I'm believe that for now the followings are incorrect.....
    switch (op_type) {
    case OP_READ: {
        switch (lock_item.lock_type) {
        case NONE:                                         return true;
        case S:
            if (lock_item.trans_holding.count(trans_id))   return true;
            else if (lock_item.queued_ops.size() == 0)     return true;
            else                                           return false;
        case X:
            if (lock_item.trans_holding.count(trans_id))   return true;
            else                                           return false;
        default: err_invalid_case();
        }
    }
    case OP_WRITE:
        switch (lock_item.lock_type) {
        case NONE:                                         return true;
        case S:
            // check if we could safely upgrade the lock
            if ((lock_item.trans_holding.size() == 1) &&
                (lock_item.trans_holding.count(trans_id))) return true;
            else                                           return false;
        case X:
            if (lock_item.trans_holding.count(trans_id))   return true;
            else                                           return false;
        default: err_invalid_case();
        }
    default: err_invalid_case();
    }
    return false;
}

