#include "DataMng.h"
#include "TransMng.h"

#include<iostream>
#include<fstream>

DataMng *DM[SITE_COUNT + 1];
TransMng *TM;

int main(int argc, char **argv) {
    // initialize TM
    TM = new TransMng();

    // initialize DM(s)
    DM[0] = nullptr;
    for (int i = 1; i <= SITE_COUNT; ++i) {
        DM[i] = new DataMng(i);
    }

    // begin main loop
    if (argc > 1) {
        std::ifstream infile(argv[1]);
        if (!infile.is_open()) {
            std::cout << "ERROR Open Input File\n";
        } else {
            TM->Simulate(infile);
        }
    } else {
        TM->Simulate(std::cin);
    }

    // clean up
    delete TM;
    for (int i = 1; i <= SITE_COUNT; ++i) {
        delete DM[i];
    }

    return 0;
}