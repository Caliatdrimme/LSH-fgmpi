# LSH-fgmpi

to run

mpiexec -nfg n_subprocess -n n_process ./lsh [params]

//ABC
mpiexec -nfg 1 -n 12 ./lsh 1 0 0 6 3 3 1 2 1 2.0 0.1 0.1 3 data.txt

//SAX
mpiexec -nfg 1 -n 12 ./lsh 2 1 0 9 3 2 1 3 2 4.5 0.1 0.1 3 data.txt

//SSH
mpiexec -nfg 1 -n 12 ./lsh 3 2 0 9 3 3 1 2 1 2.0 0.1 0.1 3 data.txt

//trial 1
//ABC
//read in elements index 0 to 5 inclusive

//1 receiver, manager, writer, similarity, hash and storage = 6 nodes
//num_hash hashtables
//rest are workers

//need at least 6 + num_hash + 1 total threads (n_subprocessXn_process)

[params]:
	trial = atoi(argv[1]);
	
	flag = atoi(argv[2]);
	
	start = atoi(argv[3]);
	elements = atoi(argv[4]);
	
	num_hash = atoi(argv[5]);
	size_hash = atoi(argv[6]);
	step_hash = atoi(argv[7]);
	
	num_symbols = atoi(argv[8]);
	word_length = atoi(argv[9]);

	average = atof(argv[10]);
	sd = atof(argv[11]);
	sim = atof(argv[12]);
	
    n = atoi(argv[13]);
	//filename = argv[14];
	
//start should be between 0 and elements
//num_hash, size_hash, step_hash, word_length, 3=<num_symbols=<15 - all much smaller than n

//average and sd come from data 	
	
//this represents one whole run of an experiment
//each parameter array starts with what experiment $trial$ this is
//followed by $flag$ of lsh method, 0 is ABC, 1 is SAX, 2 is SSH

//$start$ is the zero-based index of first element to include
//$elements$ number of elements to include (how long is the time window)

//ABC parameters flag 0
//trial 0 start elements num_hash size_hash 0 2 0 average 0 sim n "data.txt"
//$num_hash$ is number of random indexes for hashtables
//$size_hash$ is length of subsequence to use
//$average$ is the average value of the raw time series
//$sim$ is the similarity parameter 

//SAX parameters flag 1
//trial 1 start elements num_hash size_hash 0 num_symbols word_length average sd 0 n "data.txt"
//$num_hash$ is number of random indexes for hashtables
//$size_hash$ is length of subsequences to use
//$num_symbols$ is number of symbols to encode the time series
//$word_length$ is word length for 1 symbol
//$average$ and $sd$ are statistics for the raw time series to use for standarization

//SSH parameters flag 2
//trial 2 start elements num_hash size_hash step_hash num_symbols word_length 0 0 0 n "data.txt"
//$num_hash$ is the number of minhashes to use for hashtables, length for each is 2^num_symbols/4 (quater of all possible shingles/indexes)
//$size_hash$ is the length of the random vector to use for sketching
//$step_hash$ is the number of symbols to move the random sketching vector by
//$num_symbols$ is the length of a shingle
//$word_length$ is the number of symbols to move the shingles by (overlap)

//$n$ total of data items 13 and str $filename= "data.txt$ 14
