# Adv-DB project - Replicated Concurrency Control and Re-covery

Author:
- Dayou Du (dayoudu@nyu.edu)
- Shiyao Lei (sl6569@nyu.edu)

## Prerequisites

- CMake >= 3.1
- GNU Make >= 3.8
- A proper C++ compiler with c++11 support, ideally GNU g++ >= 5.3

## Build/Run Guide

### Using CMake (recommended)

1. `mkdir build`
2. `cd build`
3. `cmake ../src`
4. `make`

You will find that the executable `repcrec` being compiled

**How to run**

`repcrec` (Then the program will get input from stdin) or `repcrec <input-file>`

Some sample inputs are also provided, please try `runit.sh` in the project root directory

### Using reprounzip

You will need a vagrant Ubuntu with reprounzip installed

1. put `repcrec.rpz` into your virual machine
2. `vagrant ssh` into your virtual machine
3. `sudo reprounzip chroot setup repcrec.rpz run_tests`
4. `sudo reprounzip chroot run run_tests`

Then the packed program will run through the packed test cases in your VM
