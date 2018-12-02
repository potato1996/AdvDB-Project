#include<cstddef>
#include<cstdio>
#include<cstdlib>
#include<vector>
#include<string>
#include<iostream>

#include"TransMng.h"
#include"DataMng.h"

extern DataMng* DM[SITE_COUNT];

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

void
TransMng::Simulate(std::istream inputs) {
    std::string line_buffer;
    while (std::getline(inputs, line_buffer)) {
        auto parsed_line = parse_line(line_buffer);

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
        }
        else if (command_type == "R") {
            // R(Tn, xn)
        }
    }
}

TransMng::TransMng() {
}


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
TransMng::Read(transid_t trans_id, itemid_t item_id) {
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
    op_t read_op(_next_opid, trans_id, OP_READ, read_param);
    _next_opid++;
    if (_trans_table[trans_id].is_ronly) {
        read_op.op_type = OP_RONLY;
    }

    // 5. for a read operation, send it to any of the sites should be fine
    bool success = false;
    for (siteid_t site_id : _item_sites[item_id]) {
        if (!_site_status[site_id]) {
            // this site is down, try next one
            continue;
        }

        // 6. For Read-Only transaction
        if (read_op.op_type == OP_RONLY) {
            if (DM[site_id]->Ronly(read_op, _trans_table[trans_id].start_ts)) {
                success = true;
                break;
            }
        }
        // 7. For normal transaction (use 2pc)
        else {
            if (DM[site_id]->Read(read_op)) {
                success = true;
                break;
            }
        }
    }

    // 8. if not success, then all the sites are down or not readable, we need to queue this operation
    if (!success) {
        _queued_ops.push_back(read_op);
    }
}

void
TransMng::Write(transid_t trans_id, itemid_t item_id, int value) {
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
    

    // 5. broad to all the sites
    bool success = false;
    for (siteid_t site_id : _item_sites[item_id]) {
        if (!_site_status[site_id]) {
            // this site is down, try next one
            continue;
        }

        DM[site_id]->Write(write_op);
        success = true;
    }

    // 8. if not success, then all the sites are down we need to queue this operation
    if (!success) {
        _queued_ops.push_back(write_op);
    }
}


