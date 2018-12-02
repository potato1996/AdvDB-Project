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

    transid_t get_trans_id(std::string s) {
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
            transid_t trans_id = get_trans_id(parsed_line[1]);
            Begin(trans_id, false);
        }
        else if (command_type == "beginRO") {
            // beginRO(Tn)
            transid_t trans_id = get_trans_id(parsed_line[1]);
            Begin(trans_id, true);
        }
        else if (command_type == "end") {
            // 
        }
        else if (command_type == "W") {

        }
        else if (command_type == "R") {

        }
    }
}

TransMng::TransMng() {
}


void
TransMng::ReceiveReadResponse(op_t op, int value) {
 
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
    // 1. create the op param
    op_param_t read_param;
    read_param.r_param.item_id = item_id;

    // 2. create an op
    op_t read_op(_next_opid, trans_id, OP_READ, read_param);

    // 3. for a read operation, send it to any of the sites should be fine

}


