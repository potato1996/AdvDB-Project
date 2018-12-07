/**
 * Author: Dayou Du (dd2645@nyu.edu)
 * Date: 2018-12-06
 * Description: Transaction manager translates read and write requests on variables to read and write request on copies.
 *  -----------------------------------------------------------------------------------------
 *          name          |         Inputs       |                  output
 *  -----------------------------------------------------------------------------------------
 *  Simulate              |inputs                |
 *  -----------------------------------------------------------------------------------------
 *  ReceiveReadResponse   |site_id, value        |
 *  -----------------------------------------------------------------------------------------
 *  ReceiveWriteResponse  |site_id               |
 *  -----------------------------------------------------------------------------------------
 *  DetectDeadLock        |                      |true if there is deadlock, false otherwise
 *  -----------------------------------------------------------------------------------------
 *  TryExecuteQueue       |                      |
 *  -----------------------------------------------------------------------------------------
 *  ExecuteCommand        |line                  |
 *  -----------------------------------------------------------------------------------------
 *  Begin                 |trans_id, is_ronly    |
 *  -----------------------------------------------------------------------------------------
 *  Finish                |trans_id              |
 *  -----------------------------------------------------------------------------------------
 *  Abort                 |trans_id              |
 *  -----------------------------------------------------------------------------------------
 *  Read                  |op                    |true if it can be readed, false otherwise
 *  -----------------------------------------------------------------------------------------
 *  Ronly                 |op                    |true if it can be readed, false otherwise
 *  -----------------------------------------------------------------------------------------
 *  Write                 |op                    |true if it can be written, false otherwise
 *  -----------------------------------------------------------------------------------------
**/

#include<cstddef>
#include<cstdio>
#include<cstdlib>
#include<vector>
#include<string>
#include<iostream>

#include"TransMng.h"
#include"DataMng.h"

// The +1 will deal with the annoying 1-index
extern DataMng *DM[SITE_COUNT + 1];

// helper functions
namespace {

    void print_command_error() {
        std::cout << "ERROR: Invalid Command\n";
        std::exit(-1);
    }

    void print_abort(transid_t trans_id) {
        std::cout << "Transaction T" << trans_id << " already aborted, ignore this command\n";
    }

    void err_inconsist() {
        std::cout << "ERROR: Internal state inconsist\n";
    };

    transid_t parse_trans_id(std::string s) {
        try {
            transid_t trans_id = std::stoi(s.substr(1));
            return trans_id;
        }
        catch (...) {
            print_command_error();
            return -1;
        }
    }

    siteid_t parse_site_id(std::string s) {
        try {
            int site_id = std::stoi(s);
            if (site_id < 1 || site_id > SITE_COUNT) {
                print_command_error();
                return -1;
            }
            return site_id;
        }
        catch (...) {
            print_command_error();
            return -1;
        }
    }

    itemid_t parse_item_id(std::string s) {
        try {
            int item_id = std::stoi(s.substr(1));
            if (item_id < 1 || item_id > ITEM_COUNT) {
                print_command_error();
                return -1;
            }
            return item_id;
        }
        catch (...) {
            print_command_error();
            return -1;
        }
    }

    int parse_value(std::string s) {
        try {
            return std::stoi(s);
        }
        catch (...) {
            print_command_error();
            return 0;
        }
    }

    // this helper function will split multi commands and remove the spaces and comments
    std::vector<std::string> split_multi_command(std::string line) {
        std::vector<std::string> res;
        std::string tmp = "";
        for (int i = 0; i < line.length(); ++i) {
            if (line.substr(i, 2) == "//") {
                break;
            }
            char c = line[i];
            if (c == ';' || c == '\n') {
                res.push_back(tmp);
                tmp = "";
            } else if (c == ' ') {
                continue;
            } else {
                tmp.push_back(c);
            }
        }
        if (tmp.length() > 0) {
            res.push_back(tmp);
        }
        return res;
    }

    // seperate the input string with '(', ',' or ')'
    std::vector<std::string> parse_line(std::string line) {
        std::vector<std::string> parsed;

        std::size_t last = 0;
        std::size_t next = 0;
        while ((next = line.find_first_of("(,)", last)) != line.npos) {
            std::string tmp = line.substr(last, next - last);
            if (tmp.length() > 0) {
                parsed.push_back(tmp);
            }
            last = next + 1;
        }

        return parsed;
    }
}


TransMng::TransMng() {
    _now = 0;
    _next_opid = 0;

    // assume that all the sites are up at beginning
    for (int i = 1; i <= SITE_COUNT; ++i) {
        _site_status[i] = true;
    }

    // initialize item-site mappings
    for (siteid_t site_id = 1; site_id <= SITE_COUNT; site_id++) {
        for (itemid_t item_id = 1; item_id <= ITEM_COUNT; item_id++) {
            if ((item_id % 2 == 0) || (1 + (item_id % 10) == site_id)) {
                _item_sites[item_id].push_back(site_id);
            }
        }
    }
}

// ------------------- Main Loop -----------------------------

void
TransMng::Simulate(std::istream &inputs) {
    std::string line_buffer;
    while (true) {
        std::cout << "------------------- Time Tick: " << _now
                  << " -------------------------" << std::endl;
        // 1. At the beginning of each timestamp, detect deadlock
        while (DetectDeadLock()) {
            // 2. If we have aborted something, maybe we can execute some commands
            TryExecuteQueue();
        }

        if (!std::getline(inputs, line_buffer)) {
            break;
        }

        // 3. Split the commands read in
        std::vector<std::string> commands = split_multi_command(line_buffer);
        for (std::string command : commands) {
            ExecuteCommand(command);
        }

        // 4. Try to execute what's left in the queue
        TryExecuteQueue();

        _now++;
    }
}

void
TransMng::ExecuteCommand(std::string line) {
    auto parsed_line = parse_line(line);

    if (parsed_line.size() == 0) {
        print_command_error();
    }

    auto command_type = parsed_line[0];
    if (command_type == "begin") {
        // begin(Tn)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        Begin(trans_id, false);
    } else if (command_type == "beginRO") {
        // beginRO(Tn)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        Begin(trans_id, true);
    } else if (command_type == "end") {
        // end(Tn)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        Finish(trans_id);
    } else if (command_type == "W") {
        // W(Tn, xn, v)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        itemid_t item_id = parse_item_id(parsed_line[2]);
        int value = parse_value(parsed_line[3]);
        // 1. if this transaction is invalid, report error
        if (!_trans_table.count(trans_id)) {
            print_command_error();
            return;
        }

        // 2. this site will abort, do nothing but report it
        if (_trans_table[trans_id].will_abort) {
            print_abort(trans_id);
            return;
        }

        // 3. create the op param
        op_param_t write_param;
        write_param.w_param.item_id = item_id;
        write_param.w_param.value = value;

        // 4. Create op;
        op_t write_op(_next_opid, trans_id, OP_WRITE, write_param);
        _next_opid++;

        // 5. put it into our execution queue
        _queued_ops.push_back(write_op);
    } else if (command_type == "R") {
        // R(Tn, xn)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        itemid_t item_id = parse_item_id(parsed_line[2]);
        // 1. if this transaction is invalid, report error
        if (!_trans_table.count(trans_id)) {
            print_command_error();
            return;
        }

        // 2. this site will abort, do nothing but report it
        if (_trans_table[trans_id].will_abort) {
            print_abort(trans_id);
            return;
        }

        // 3. create the op param
        op_param_t read_param;
        read_param.r_param.item_id = item_id;

        // 4. Create op;
        if (_trans_table[trans_id].is_ronly) {
            op_t read_op(_next_opid, trans_id, OP_RONLY, read_param);
            _next_opid++;

            // 5. put it into our execution queue
            _queued_ops.push_back(read_op);
        } else {
            op_t read_op(_next_opid, trans_id, OP_READ, read_param);
            _next_opid++;

            // 5. put it into our execution queue
            _queued_ops.push_back(read_op);
        }
    } else if (command_type == "fail") {
        siteid_t site_id = parse_site_id(parsed_line[1]);
        Fail(site_id);
    } else if (command_type == "recover") {
        siteid_t site_id = parse_site_id(parsed_line[1]);
        Recover(site_id);
    } else if (command_type == "dump") {
        if (parsed_line.size() == 1) {
            // dump()
            DumpAll();
        } else if (parsed_line.size() == 2) {
            if (parsed_line[1][0] == 'x') {
                // dump(xi)
                itemid_t item_id = parse_item_id(parsed_line[1]);
                DumpItem(item_id);
            } else {
                // dump(s)
                siteid_t site_id = parse_site_id(parsed_line[1]);
                DumpSite(site_id);
            }
        } else {
            print_command_error();
        }
    } else {
        print_command_error();
    }
}


// -------------------- Tester Cause Events -----------------------

void
TransMng::Fail(siteid_t site_id) {
    if (_site_status[site_id]) {
        // fail the DM
        DM[site_id]->Fail();
        _site_status[site_id] = false;

        // Abort the 2pc transactions that accessed this site so far
        for (auto &p : _trans_table) {
            if ((!p.second.is_ronly)
                && (!p.second.will_abort)
                && (p.second.visited_sites.count(site_id))) {
                std::cout << "Transaction T" << p.first
                          << " aborted, because it has accessed Site " << site_id
                          << " and this site failed\n";
                Abort(p.first);
            }
        }
    } else {
        std::cout << "Site " << site_id << " is not up yet\n";
    }
}

void
TransMng::Recover(siteid_t site_id) {
    DM[site_id]->Recover(_now);
    _site_status[site_id] = true;
}

void
TransMng::DumpAll() {
    for (siteid_t site_id = 1; site_id <= SITE_COUNT; site_id++) {
        DumpSite(site_id);
    }
}

void
TransMng::DumpSite(siteid_t site_id) {
    DM[site_id]->Dump();
}

void
TransMng::DumpItem(itemid_t item_id) {
    for (siteid_t site_id : _item_sites[item_id]) {
        DM[site_id]->DumpItem(item_id);
    }
}

// -------------------- Transaction Execution Events -------------------------

void
TransMng::TryExecuteQueue() {
    std::list<op_t> new_queue;
    while (!_queued_ops.empty()) {
        op_t op = _queued_ops.front();
        _queued_ops.pop_front();
        if (_trans_table[op.trans_id].will_abort) {
            // this transaction has already aborted, ignore
            continue;
        }
        switch (op.op_type) {
            case OP_READ: {
                if (!Read(op)) {
                    new_queue.push_back(op);
                }
                break;
            }
            case OP_WRITE: {
                if (!Write(op)) {
                    new_queue.push_back(op);
                }
                break;
            }
            case OP_RONLY: {
                if (!Ronly(op)) {
                    new_queue.push_back(op);
                }
                break;
            }
            default: {
                std::cout << "ERROR: Invalid case\n";
                std::exit(-1);
            }
        }
    }
    _queued_ops.swap(new_queue);
}

void
TransMng::ReceiveReadResponse(op_t op, siteid_t site_id, int value) {
    std::cout << "Received from Site " << site_id
              << " READ operation result on Transaction T" << op.trans_id
              << " | OPid: " << op.op_id
              << " | Key = " << op.param.r_param.item_id
              << " | Value = " << value
              << std::endl;
}

void
TransMng::ReceiveWriteResponse(op_t op, siteid_t site_id) {
    std::cout << "Received from Site " << site_id
              << " WRITE operation result on Transaction T" << op.trans_id
              << " | OPid: " << op.op_id
              << " | Key = " << op.param.w_param.item_id
              << " | Value = " << op.param.w_param.value
              << std::endl;
}

void
TransMng::Begin(transid_t trans_id, bool is_ronly) {
    if (_trans_table.count(trans_id)) {
        print_command_error();
    }
    _trans_table[trans_id] = trans_table_item(_now, is_ronly);
}

void
TransMng::Finish(transid_t trans_id) {
    // The instruction assume that the next command will not arrive if there are pending operations
    if (_trans_table[trans_id].will_abort) {
        std::cout << "Transaction T" << trans_id << " has already aborted\n";
    } else {
        for (siteid_t site_id = 1; site_id <= SITE_COUNT; site_id++) {
            DM[site_id]->Commit(trans_id, _now);
        }
        std::cout << "Transaction T" << trans_id << " finished succesfully!\n";
    }
    _trans_table.erase(trans_id);
}

void
TransMng::Abort(transid_t trans_id) {
    if (_trans_table[trans_id].will_abort) {
        // already aborted, do nothing
    } else {
        for (siteid_t site_id = 1; site_id <= SITE_COUNT; site_id++) {
            DM[site_id]->Abort(trans_id);
        }
        _trans_table[trans_id].will_abort = true;
    }
}

bool
TransMng::Read(op_t op) {
    itemid_t item_id = op.param.r_param.item_id;
    // for a read operation, send it to any of the sites should be fine
    for (siteid_t site_id : _item_sites[item_id]) {
        if (!_site_status[site_id]) {
            // this site is down, try next one
            continue;
        }

        if (DM[site_id]->GetReadLock(op.trans_id, item_id)) {
            // let DM execute it
            if (DM[site_id]->Read(op)) {
                _trans_table[op.trans_id].visited_sites.insert(site_id);
                return true;
            } else {
                err_inconsist();
            }
        }
    }

    // If not success, then all the sites are down or not readable, we need to queue this operation
    return false;
}

bool
TransMng::Ronly(op_t op) {
    itemid_t item_id = op.param.r_param.item_id;
    transid_t trans_id = op.trans_id;
    timestamp_t start_ts = _trans_table[trans_id].start_ts;
    // for a read operation, send it to any of the sites should be fine
    for (siteid_t site_id : _item_sites[item_id]) {
        if (!_site_status[site_id]) {
            // this site is down, try next one
            continue;
        }

        // let DM execute it
        if (DM[site_id]->Ronly(op, start_ts)) {
            return true;
        }
    }

    // If not success, then all the sites are down or not readable, we need to queue this operation
    return false;
}

bool
TransMng::Write(op_t op) {
    transid_t trans_id = op.trans_id;
    itemid_t item_id = op.param.w_param.item_id;
    int value = op.param.w_param.value;

    // 5. broadcase to all the sites
    bool success = true;
    for (siteid_t site_id : _item_sites[item_id]) {
        if (!_site_status[site_id]) {
            // this site is down, try next one
            continue;
        }

        success &= DM[site_id]->GetWriteLock(trans_id, item_id);
    }

    if (success) {
        for (siteid_t site_id : _item_sites[item_id]) {
            if (!_site_status[site_id]) {
                // this site is down, try next one
                continue;
            }

            DM[site_id]->Write(op);
            _trans_table[trans_id].visited_sites.insert(site_id);
        }
    }

    return success;
}


namespace {
    // dfs-based cycle detector
    bool dfs_cycle(transid_t curr,
                   transid_t root,
                   std::unordered_map<transid_t, std::unordered_set<transid_t>> &graph,
                   std::unordered_set<transid_t> &mark_global) {
        mark_global.insert(curr);
        for (transid_t child : graph[curr]) {
            if (child == root) {
                return true;
            }
            if (!mark_global.count(child)) {
                if (dfs_cycle(child, root, graph, mark_global)) {
                    return true;
                }
            }
        }
        return false;
    }
}

bool
TransMng::DetectDeadLock() {

    // 1. get all the locks waiting graphs from the DMs
    std::unordered_map<siteid_t, std::unordered_set<siteid_t>> waiting_graph;
    for (siteid_t site_id = 1; site_id <= SITE_COUNT; site_id++) {
        if (_site_status[site_id]) {

            // get the waiting graph from each site
            auto site_waiting_graph = DM[site_id]->GetWaitingGraph();

            // now merge the graphs
            for (auto &p : site_waiting_graph) {
                waiting_graph[p.first].insert(p.second.begin(), p.second.end());
            }
        }
    }

    // 2. find the oldest transaction that in a cycle
    int oldest = -1;
    siteid_t oldest_transid = -1;
    for (const auto &p : waiting_graph) {
        std::unordered_set<transid_t> mark_global;
        if (oldest < _trans_table[p.first].start_ts &&
            dfs_cycle(p.first, p.first, waiting_graph, mark_global)) {

            oldest = _trans_table[p.first].start_ts;
            oldest_transid = p.first;
        }
    }

    if (oldest_transid != -1) {
        std::cout << "Transaction T" << oldest_transid << " aborted because of deadlock\n";
        Abort(oldest_transid);
        return true;
    }

    return false;
}