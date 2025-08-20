## DPOpt: Differentially Private Query Optimization
### Secure implementations of operators obtained from SPECIAL: Synopsis Assisted Secure Collaborative Analytics

### Dependencies
1. [EMPtool](https://github.com/emp-toolkit/emp-tool)
2. [Emp-sh2pc](https://github.com/emp-toolkit/emp-sh2pc)


### Compilation
After you have installed dependencies, switch to the root directory of SPECIAL and run the following in the specified order:

1. `cmake -DCMAKE_CXX_FLAGS="-I/opt/homebrew/opt/boost/include" -DCMAKE_EXE_LINKER_FLAGS="-L/opt/homebrew/opt/boost/lib"`

2. `make`

### Test
* IF you want to test the code, type

   `./bin/[binaries] [port number] 1 & ./bin/[binaries] [port number] 2`

* Examples: test filter operator, type

	`./bin/test_filter 1 12345 & ./bin/test_filter 2 12345`

* Test differentially private query optimizer and executor
   `./bin/test_qplan 1 12345 & ./bin/test_qplan 2 12345`

	
### Question
Please send email to sara.alam@tufts.edu
