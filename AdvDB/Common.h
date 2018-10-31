#pragma once
#define CITE_COUNT 10


typedef int transid_t;
typedef int siteid_t;
typedef int itemid_t;
typedef int opid_t;
typedef int timestamp_t;

// For now there are only two types of operation
enum op_type_t {
	READ,
	WRITE
};

// A write operation have two params: x, v 
struct w_param_t {
	itemid_t item_id;
	int		 value;
};

// A read operation only have one param
struct r_param_t {
	itemid_t item_id;
};

union op_param_t {
	w_param_t w_param;
	r_param_t r_param;
};

// An operation looks like: W1(x, v) or R1(x)
struct op_t{
	transid_t	 trans_id;
	op_type_t	 op_type;
	op_param_t	 param;
};