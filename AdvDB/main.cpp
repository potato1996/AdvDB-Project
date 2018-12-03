#include "DataMng.h"
#include "TransMng.h"
#include "Common.h"

#include<iostream>

DataMng* DM[SITE_COUNT + 1];
TransMng* TM;

int main() {
    // initialize TM
    TM = new TransMng();

    // initialize DM(s)
    DM[0] = nullptr;
    for (int i = 1; i <= SITE_COUNT; ++i) {
        DM[i] = new DataMng(i);
    }

    // begin main loop
    TM->Simulate(std::cin);

    // clean up
    delete TM;
    for (int i = 1; i <= SITE_COUNT; ++i) {
        delete DM[i];
    }

    return 0;
}