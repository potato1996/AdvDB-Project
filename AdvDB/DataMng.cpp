#include "DataMng.h"

#include "Common.h"
#include "TransMng.h"

extern TransMng* TM;

namespace {
    bool is_replicated(itemid_t item_id) {
        return item_id % 2 == 0;
    }
} // helper functions


DataMng::DataMng(siteid_t site_id) {
    // basic stuff
    _site_id = site_id;
    _is_up = true;

    // initialize the data items
    for (itemid_t item_id = 1; item_id <= ITEM_COUNT; item_id++) {
        if ((item_id % 2 == 0) || (1 + (item_id % 10) == site_id)) {
            // commit time = 0 means initial values
            _disk[item_id].push_back(disk_item(item_id * 10, 0));

            // since we have small number of data items, we can cache all of them in memory
            _memory[item_id] = mem_item(item_id * 10);
            _readable[item_id] = true;

        }
    }
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
DataMng::Recover() {
    _is_up = true;

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

    if (check_conflict(item_id, trans_id, OP_READ)) {
        // update the lock table
        lock_table_item& lock_item = _lock_table[item_id];
        if (lock_item.lock_type == NONE) {
            lock_item.lock_type = S;
        }
        lock_item.trans_holding.insert(trans_id);

        // execute the operation
        int value = _memory[item_id].value;

        // send the result back to TM
        TM->ReceiveResponse(op, value);
    }
    else {
        // append this operation to the end of the lock queue
        _lock_table[item_id].queued_ops.push_back(op);
    }

    return true;
}

bool
DataMng::Ronly(op_t op, timestamp_t ts) {
    itemid_t  item_id  = op.param.r_param.item_id;
    transid_t trans_id = op.trans_id;
    if (!_readable[item_id]) {
        return false;
    }

    // go through the disk, find the latest commit before this ts
    const auto& value_list = _disk[item_id];
    for (auto it = value_list.begin(); it != value_list.end(); it++) {
        if (it->commit_time <= ts) {
            TM->ReceiveResponse(op, it->value);
            break;
        }
    }

    return true;
}

void
DataMng::Write(op_t op) {
    itemid_t  item_id   = op.param.w_param.item_id;
    int       write_val = op.param.w_param.value;
    transid_t trans_id  = op.trans_id;

    if (check_conflict(item_id, trans_id, OP_WRITE)) {
        // upgrade the lock type
        lock_table_item& lock_item = _lock_table[item_id];
        if (lock_item.lock_type == S || lock_item.lock_type == NONE) {
            lock_item.lock_type = X;
        }
        lock_item.trans_holding.insert(trans_id);

        // execute the operation
        _memory[item_id].value = write_val;
    }
    else {
        // append this operation to the end of the lock queue
        _lock_table[item_id].queued_ops.push_back(op);
    }
}

void
DataMng::Commit(transid_t trans_id, timestamp_t commit_time) {

}

bool
DataMng::CheckCommit(transid_t trans_id) {

}




bool 
DataMng::check_conflict(itemid_t item_id, transid_t trans_id, op_type_t op_type) {
    const lock_table_item& lock_item = _lock_table[item_id];
    if (op_type == OP_READ) {
        if (lock_item.lock_type == NONE) {
            return true;
        }
        else if(lock_item.lock_type == S) {
            if (lock_item.trans_holding.count(trans_id)) {
                return true;
            }
            else if (lock_item.queued_ops.size() == 0) {
                return true;
            }
            else {
                return false;
            }
        }
        else {
            // lock_item.lock_type == X
            if (lock_item.trans_holding.count(trans_id)) {
                return true;
            }
            else {
                return false;
            }
        }
    }
    else {
        // op_type == OP_WRITE
        if (lock_item.lock_type == NONE) {
            return true;
        }
        else if (lock_item.lock_type == S) {
            // check if we could upgrade the lock
            if (lock_item.trans_holding.size() == 1 && lock_item.trans_holding.count(trans_id)) {
                return true;
            }
            else {
                return false;
            }
        }
        else {
            // lock_item.lock_type == X
            if (lock_item.trans_holding.count(trans_id)) {
                return true;
            }
            else {
                return false;
            }
        }
    }
}

