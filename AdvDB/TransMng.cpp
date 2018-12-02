#include<cstddef>
#include<cstdio>
#include<cstdlib>
#include<vector>
#include<string>
#include<iostream>

#include"TransMng.h"
#include"DataMng.h"


// The +1 will deal with the annoying 1-index
extern DataMng* DM[SITE_COUNT + 1];

// helper functions
namespace {

    void print_command_error() {
        std::cout << "ERROR: Invalid Command\n";
        std::exit(-1);
    }

    void print_abort(transid_t trans_id) {
        std::cout << "Transaction T" << trans_id << " will abort, ignore this command\n";
    }

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

    itemid_t parse_item_id(std::string s){
        try {
            int item_id = std::stoi(s.substr(1));
            if(item_id < 1 || item_id > ITEM_COUNT){
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

    int parse_value(std::string s){
        try{
            return std::stoi(s);
        }
        catch (...) {
            print_command_error();
            return 0;
        }
    }

    // seperate the input string with '(', ',' or ')'
    std::vector<std::string> parse_line(std::string line) {
        std::vector<std::string> parsed;
        
        std::size_t last = 0; 
        std::size_t next = 0; 
        while ((next = line.find_first_of("(,)", last)) != line.npos) { 
            parsed.push_back(line.substr(last, next - last)); 
            last = next + 1; 
        }
      
        return parsed;
    }
} 


TransMng::TransMng() {
    _now       = 0;
    _next_opid = 0;
    
    // assume that all the sites are up at beginning
    for(int i = 0; i < SITE_COUNT; ++i){
        _site_status[i] = true;
    }

    // initialize item-site mappings
    for(siteid_t site_id = 1; site_id <= SITE_COUNT; site_id++){
        for(itemid_t item_id = 1;  item_id <= ITEM_COUNT; item_id++){
            if((item_id % 2 == 0) || (1 + (item_id % 10) == site_id)){
                _item_sites[item_id].push_back(site_id);
            }
        }
    }  
}

// ------------------- Main Loop -----------------------------

void
TransMng::Simulate(std::istream inputs) {
    std::string line_buffer;
    while (std::getline(inputs, line_buffer)) {

        ExecuteCommand(line_buffer);
    }
}

void 
TransMng::ExecuteCommand(std::string line){
    auto parsed_line = parse_line(line);

    if (parsed_line.size() == 0) {
        print_command_error();
    }

    auto command_type = parsed_line[0];
    if (command_type == "begin") {
        // begin(Tn)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        Begin(trans_id, false);
    }
    else if (command_type == "beginRO") {
        // beginRO(Tn)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        Begin(trans_id, true);
    }
    else if (command_type == "end") {
        // end(Tn)
    }
    else if (command_type == "W") {
        // W(Tn, xn, v)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        itemid_t  item_id  = parse_item_id(parsed_line[2]);
        int       value    = parse_value(parsed_line[3]);
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

        // 5. Try to execute it
        if(!Write(write_op)){
            // if not successfule, queue it
            _queued_ops.push_back(write_op);
        }
    }
    else if (command_type == "R") {
        // R(Tn, xn)
        transid_t trans_id = parse_trans_id(parsed_line[1]);
        itemid_t  item_id  = parse_item_id(parsed_line[2]);
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
            // 5. Try to execute it
            if(!Ronly(read_op)){
                // if not sucessful, queue it
                _queued_ops.push_back(read_op);
            }
        }
        else{
            op_t read_op(_next_opid, trans_id, OP_READ, read_param);
            _next_opid++;
            // 5. Try to execute it
            if(!Read(read_op)){
                // if not successful, queue it
                _queued_ops.push_back(read_op);
            }
        }
    }
}


// -------------------- Tester Cause Events -----------------------

void 
TransMng::Fail(siteid_t site_id){
    if(_site_status[site_id]){
        // fail the dm
        DM[site_id]->Fail();

        // Abort the 2pc transactions that accessed this site so far
        for(auto &p: _trans_table){
            if((!p.second.is_ronly)
               && (!p.second.will_abort)
               && (p.second.visited_sites.count(site_id))){
                std::cout << "Transaction T" << p.first 
                    << " aborted, because it has accessed Site " << site_id
                    << " and this site failed\n";
                Abort(p.first);
            }
        }
    }
    else{
        std::cout << "Site " << site_id << " is not up yet\n";
    }
}

void
TransMng::Recover(siteid_t site_id){
    DM[site_id]->Recover(_now);
}

void 
TransMng::DumpAll(){
    for(siteid_t site_id = 1; site_id <= SITE_COUNT; site_id++){
        DumpSite(site_id);
    }
}

void 
TransMng::DumpSite(siteid_t site_id){
    DM[site_id]->Dump();
}

void
TransMng::DumpItem(itemid_t item_id){
    for(siteid_t site_id: _item_sites[item_id]){
        DM[site_id]->DumpItem(item_id);
    }
}

// -------------------- Transaction Execution Events -------------------------

void
TransMng::ReceiveReadResponse(op_t op, int value) {
    std::cout << "Received response for read operation on Transaction T" << op.trans_id
        << " OP id: " << op.op_id
        << " | Key = " << op.param.r_param.item_id
        << " | Value = " << value
        << std::endl;
}

void
TransMng::ReceiveWriteResponse(op_t op) {
    std::cout << "Received response for write operation on Transaction T" << op.trans_id
        << " OP id: " << op.op_id
        << " | Key = " << op.param.w_param.item_id
        << " | Value = " << op.param.w_param.value
        << std::endl;
}

timestamp_t
TransMng::QueryTransStartTime(transid_t trans_id) {
    if (!_trans_table.count(trans_id)) {
        std::cout << "ERROR: Transaction T" << trans_id << " is not active";
        std::exit(-1);
    }
    else {
        return _trans_table[trans_id].start_ts;
    }
}

void
TransMng::Begin(transid_t trans_id, bool is_ronly) {
    if (_trans_table.count(trans_id)) {
        print_command_error();
    }
    _trans_table[trans_id] = trans_table_item(trans_id, is_ronly);
}

void
TransMng::Finish(transid_t trans_id){
    // The instruction assume that the next command will not arrive if there are pending operations
    if(_trans_table[trans_id].will_abort){
        std::cout << "Transaction T" << trans_id << " has already aborted\n";
    }
    else {
        for(siteid_t site_id: _trans_table[trans_id].visited_sites){
            DM[site_id]->Commit(trans_id, _now);
        }
    }
}

void
TransMng::Abort(transid_t trans_id){
    if(_trans_table[trans_id].will_abort){
        // already aborted, do nothing
    }
    else{
        for(siteid_t siteid_t: _trans_table[trans_id].visited_sites){
            DM[siteid_t]->Abort(trans_id);
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

        // let DM execute it
        if(DM[site_id]->Read(op)) {
            _trans_table[op.trans_id].visited_sites.insert(site_id);
            return true;
        }
    }

    // If not success, then all the sites are down or not readable, we need to queue this operation
    return false;
}

bool
TransMng::Ronly(op_t op) {
    itemid_t    item_id  = op.param.r_param.item_id;
    transid_t   trans_id = op.trans_id;
    timestamp_t start_ts = _trans_table[trans_id].start_ts;
    // for a read operation, send it to any of the sites should be fine
    for (siteid_t site_id : _item_sites[item_id]) {
        if (!_site_status[site_id]) {
            // this site is down, try next one
            continue;
        }

        // let DM execute it
        if(DM[site_id]->Ronly(op, start_ts)) {
            return true;
        }
    }

    // If not success, then all the sites are down or not readable, we need to queue this operation
    return false;
}

bool
TransMng::Write(op_t op) {
    transid_t trans_id = op.trans_id;
    itemid_t  item_id  = op.param.w_param.item_id;
    int       value    = op.param.w_param.value;

    // 5. broadcase to all the sites
    bool success = false;
    for (siteid_t site_id : _item_sites[item_id]) {
        if (!_site_status[site_id]) {
            // this site is down, try next one
            continue;
        }

        DM[site_id]->Write(op);
        _trans_table[trans_id].visited_sites.insert(site_id);
        success = true;
    }

    return success;
}


