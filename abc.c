/*Parallel LSH for network discovery
ABC 
January 15 2021
Svetlana Sodol 
UBC
*/


#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


/******* FG-MPI Boilerplate begin *********/
#include "fgmpi.h"
int my_main( int argc, char** argv ); /*forward declaration*/
FG_ProcessPtr_t binding_func(int argc, char** argv, int rank){
return (&my_main);
}
FG_MapPtr_t map_lookup(int argc, char** argv, char* str){
return (&binding_func);
}
int main( int argc, char *argv[] )
{
FGmpiexec(&argc, &argv, &map_lookup);
return (0);
}
/******* FG-MPI Boilerplate end *********/

/******* MANAGER *********/
//receives elements from receiver
//once receives enough elements for a subsequence 
//assigns worker nodes to handle this subsequence in hashing
//and sends to each the subsequence and index of hash function to use
//flags the first worker as the one to hold the whole processed data item
//sends this index to the workers along
//INPUTS: flag, elements, num_hash, size_hash, step_hash
//INCOMING:
//OUTGOING:
//tag 0 to shut nodes down
//tag 5 to receive stream
//tag 1 for sim pair
//tag 2 for hash bucket
//tag 3 for full preprocessed data item
//tag 4 is the hash function
//tag 6 is SAX breakpoints
//tag 7 is SAX distances
//tag 8 is manager and worker assignment communication
//tag 9 is data item, its primary worker rank and its hash code
void manager_fn(int flag, int elements, int num_hash, int size_hash, int step_hash, int size) {
    int data;
    float n;
    float[elements] item;
    //current data item index
    int count= 0;
    //current index of the primary worker for the current data item
    int primary;
    //start index of the current subsequence 
    int start;
    
    int tag;
    
    //how many elements in the subsequence
    int num;
    
    //use as flag for shutting down
    int shut = 0;

    
    while(1){
    
        //receive this data item
        for (int j = 0; j < elements; j++){
            MPI_Recv(&data, 1, MPI_FLOAT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (tag == 0){shut = 1;
            break;
            }//shut down
            item[j] = data;
         }//for receiving the data item
        
        if (shut == 1){break;}//shut down
    
        for (int i = 0; i < num_hash; i++){
        
            //calculate start index for this hashes subsequence
            
            if (flag == 2){start = 0; num = elements}//if SSH send whole data item, the hash is the vector applied to whole thing 
            else {start = i*step_hash; num = size_hash}//else we move the start step_hash steps for each new hash
            
            //worker sends data == 0 with tag 8 to ask for primary assignment
            //worker sends data == 1 with tag 8 to ask for secondary assignment
            MPI_Recv(&data, 1, MPI_INT, dest, 8, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
            if (data == 0){
                primary = dest;
        
            }//if primary assignment

            else if (data == 1){
                //send the primary worker index to the worker when its not the primary for this data
                MPI_Send($primary, 1, MPI_INT, dest, 8, MPI_COMM_WORLD);
            }//else if non-primary assignment  
            
            //send the data item index to the worker
            MPI_Send($count, 1, MPI_INT, dest, 8, MPI_COMM_WORLD);
            
            
            //send the subsequnce to the worker
            for (int k = 0; k < num; k++){
                MPI_Send($item[start+k], 1, MPI_FLOAT, dest, 8, MPI_COMM_WORLD);
            
            }//for sending subsequence
            
            //send the hash index
            MPI_Send((i, 1, MPI_INT, dest, 8, MPI_COMM_WORLD);
    
    }//while for each data item
    
    //start shut down procedure
    //receiver size-1 has already shut down
    //manager will shut down once all code below is done
    //similarity awaits message with tag 0 MPI_INT
    //workers awaits message with tag 0 MPI_INT
    //hash awaits message with tag 0 MPI_INT
    
    for (int k = 1; k <size-2; k++){
        //send MPI_INT message with tag 0 to shut down
        MPI_Send(1, 1, MPI_INT, k, 0, MPI_COMM_WORLD);
    }//for shutting down
    
    
    //writer awaits message with tag 0 MPI_INT - needs to finish after written all hashtables though
    //send message to hashtables that we are done
    //they will send their stuff to writer
    //once finish sending send a message to manager and shut down
    //once all hashtables reported to be done, send message to writer to shut down
    
    for (int k = 0; k <num_hash; k++){
        MPI_Recv(&data, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    }//for receiving hashtables are done
    
    MPI_Send(1, 1, MPI_INT, size-2, 0, MPI_COMM_WORLD);
    //writer is size-2
    //hashtables are 1 through num_hash
    
    
    

}//manager_fn

/******* RECEIVER *********/
//streams data from file element by element to manager
//file name and location pre-determined and hard coded here
//manager has known node rank of 0
//INPUTS: start, elements
//tag 5 is stream
void receiver_fn(int start, int elements){

	float data;
	
	//open file for reading
	FILE *fp;
	fp = fopen("data.txt", "r");
	
	//index of current item
	int count;
	count = 0;
	
	int c = getc(fp);
	
	while(data != EOF){
	
	    if (start <= count <= start+elements-1) {
	        //send this data to manager rank 0
		    if (c == "\n") {
		        //reset count back to zero as we are starting new data item
		        count = -1;
		
		    } //if new line

		    else {
		        data = (float) (c);
		        //send to manager rank 0 tag 5
		        MPI_Send(&data, 1, MPI_FLOAT, 0, 5, MPI_COMM_WORLD);
		    } //else 
	
	    }//if 

        //send note to manager that we are done reading the data items
        //tag 0 for shutting down        
        MPI_Send($data, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD);
	
	    count = count +1;
	    
	    c = getc(fp);
	
	
			
	}//while

	fclose(fp);
}//receiver_fn


/******* WRITER *********/
//writes results sent to it to file
//location and name of file hardcoded here
//form: index of data item 1, index of data item 2, similarity
//or at end of all processes:
//hash index, hash code, list of all data item indexes 
//tag 0 is shut down
//tag 1 is sim pair
//tag 2 is hash bucket
void writer_fn(int trial, int flag){
    //create results file
    FILE *fp;
    
    char name[20]
    
    if (flag == 0) {
        //ABC
        strcat(name, "ABC");
    }
    else if (flag == 1) {
        //SAX
        strcat(name, "SAX");
    }
    else if (flag == 2) {
        //SSH
        strcat(name, "SSH");
    }
    
    sprintf(a, "%d", trial);
    strcat(name, append);
    strcat(name, "txt");
    
    fp = fopen(name,"w");
    
    int data, tag, count;
    
    count = 0;
    
    while (1){
    
    
        MPI_Recv(&data, 1, MPI_INT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        if (tag == 0){
            break;
        }//if tag is 0 break
        
        else if (tag == 1){
            int ind2;
            float sim;
            fprintf(fp,"%d",data);
            MPI_Recv(&ind2, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            fprintf(fp, "%s", "\s");
            fprintf(fp,"%d",ind2);
            MPI_Recv(&sim, 1, MPI_FLOAT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            fprintf(fp, "%s", "\s");
            fprintf(fp,"%d",sim);
            
            
        
        }//if tag is 1 receive sim 
        
        else if (tag == 2){
            int ind, size, elem;
          
            MPI_Recv(&ind, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            fprintf(fp,"%d", ind);
            fprintf(fp, "%s", "\s");
            
            MPI_Recv(&size, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            for (int i = 0; i < size; i++){
                MPI_Recv(&elem, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                fprintf(fp, "%d", elem);
               
            
            }//for hash code
            
            fprintf(fp, "%s", "\s");
            
            for (int i = 0; i < data; i++){
                MPI_Recv(&elem, 1, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                fprintf(fp,"%d", elem);
            
            }//for hash bucket
        
        
        }//if tag is 2 receive hash bucket
        
        //print new line after each message written 
        fprintf(fp, "%s", "\n");
    
    //receive message 
    //write the message to the file
    //form int int float
    
    //once special message received
    //receives int, string, list of int
    //writes it to file
    
    //once end message received
    //close file and return
    }//while
    
    fclose(fp);
}//writer_fn

/******* SIMILARITY *********/

//FROM PYTHON
//calculates the similarity by ABC
float ABC_sim(int[] item1, int[] item2, float sim){

//from python
//def distance_abc(first, second):
//    c = 0
//    similarity = 0
//    for i in range(0,n):
//        if first[i] == second[i]:
//            similarity = similarity + (1 + alpha)**c
//            c = c + 1
//        else:
//            c = 0
//    return similarity

}//ABC_sim

//FROM PYTHON
//calculates the similarity by SAX
float SAX_sim(int[] item1, int[] item2, int n, int w){

//from python
//def distance_sax(first, second):
//    one = math.sqrt(n/w)
//
//    sum = 0
//    for i in range(0,w):
//      # print(first)
//       # print(first[i])
//
//        f = ind[first[i]]
//        s = ind[second[i]]
//        sum = sum + dist[f][s]**2
//        
 //   two = math.sqrt(sum)
//    
 //   return one*two

}//SAX_sim

//FROM PYTHON
//calculates the similarity by SSH
float SSH_sim(int[] item1, int[] item2){

//from python
//def distance_ssh(first, second):
//    #return jaccard sim between the two 
//    #figure out this stuff here
//    s1 = set(first)
//    s2 = set(second)
//
//   return len(s1.intersection(s2)) / len(s1.union(s2))


}//SSH_sim

//recieves two indexes of data items and indexes of worker nodes that hold them
//gets the data items and calculates the similarity
//sends info along to writer
//INPUTS: sim, flag, elements, num_symbols, word_length
//INCOMING:
//OUTGOING:
//tag 0 is shut down
//tag 1 is sim pair
void similarity_fn(int flag, int elements, int num_symbols, int word_length, float sim, int size){


    int tag, data;
    
    MPI_Recv(&data, 1, MPI_INT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    while (tag != 0){
    
        int ind1, ind2, rank1, rank2;
    
        //save first index
        ind1 = data;
        //receive its primary workers rank
        MPI_Recv(&rank1, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //receive second index
        
        MPI_Recv(&ind2, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&rank2, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        if (flag == 1){
           elems = elements/word_length;
        
        }//lower length of preprocessed data item if SAX
        
        else {
            elems = elements;
        }//else if ABC or SSH
        
        //get item1
        
        //send request to rank1
        MPI_Send(&ind1, 1, MPI_INT, rank1, 3, MPI_COMM_WORLD);
        
        //malloc item1
        int *item1;
	    item1 = (int *)malloc(sizeof(int)*elems);
        
        //receive elements elements with tag 3 and store as item1
        for (int i = 0; i < elems; i++){
            MPI_Recv(&item1[i], 1, MPI_INT, rank1, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        }//for getting item1
        
        
        //get item2
        
        //send request to rank1
        MPI_Send(&ind2, 1, MPI_INT, rank2, 3, MPI_COMM_WORLD);
        
        //malloc item1
        int *item2;
	    item2 = (int *)malloc(sizeof(int)*elems);
        
        //receive elements elements with tag 3 and store as item2
        for (int i = 0; i < elems; i++){
            MPI_Recv(&item2[i], 1, MPI_INT, rank2, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        }//for getting item2
        
        
        //calc the sim
        float sim;
        if (flag == 0) {
            //ABC
            sim = ABC_sim(item1, item2, sim);
        }
        else if (flag == 1) {
            //SAX
            sim = SAX_sim(item1, item2, elements, elems);
        
        }
        else if (flag == 2) {
            //SSH
            sim = SSH_sim(item1, item2);
        
        }
        
        //send the sim pair to writer rank size-2 with tag 1    
        MPI_Send(&ind1, 1, MPI_INT, size-2, 1, MPI_COMM_WORLD);
        MPI_Send(&ind2, 1, MPI_INT, size-2, 1, MPI_COMM_WORLD);
        MPI_Send(&sim, 1, MPI_FLOAT, size-2, 1, MPI_COMM_WORLD);
    
        //to start next cycle properly
        MPI_Recv(&data, 1, MPI_INT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
    }//while tag is not 0
    

}


/******* HASH *********/
//FROM PYTHON?
int[] random_indexes(int n, int elements){

    //return array of size n
    //holds random indexes between 0 and elements
    
}//random_indexes

//FROM PYTHON?
float[][] random_vectors(int num, int size){
    //return array of num random vectors
    //each of size size

}//random_vectors


//NEED TO WRITE FROM SCRATCH
int[] create_breakpoints(int num_symbols){

    //return array of size num_symbols-1
    //holds breakpoints as values
    //to have equal areas under normal curve

}//create_breakpoints


//FROM PYTHON
int[][] create_distances(){
    //return num_symbolsXnum_symbols array that holds distances between symbols
    //under the normal curve
    
}//create_distances

//creates the hash functions
//passes them when requested
//INPUTS: flag, elements, num_hash, size_hash, num_symbols, word_length
//INCOMING:
//OUTGOING:
//use tag 4 for hash function
//tag 0 is shut down
//tag 6 for SAX breakpoints
//tag 7 for SAX distances
void hash_fn(int flag, int elements, int num_hash, int size_hash, int num_symbols, int word_length){

    //create the necessary hash functions
    //for ABC it is num_hash indexes between start and start+elements-1 
    //for SAX it is num_hash indexes between 0 and elements/word_length
    //for SSH it is num_hash random vectors of length size_hash
    
    //for SAX need to create table of breakpoints tag 6
    //and precalculate distances tag 7
    
    
    if (flag == 0){
    
        int[num_hash] hash = random_indexes(num_hash, elements);
    
    }//ABC needs num_hash random indexes between 0 and elements
    
    else if (flag == 1){
         int[num_hash] hash = random_indexes(num_hash, elements/word_length;
         
         //create breakpoints
         //create distances
         
         int[num_symbols-1] breakpoints = create_breakpoints(num_symbols);
         
         int[num_symbols][num_symbols] distances = create_distancecs(num_symbols);
         
    }//SAX needs num_hash random indexes between 0 and elements/word_length
    
    else if (flag == 2){
    
        float[num_hash][size_hash] vectors = random_vectors(num_hash, size_hash);
    
    }//SSH needs num_hash random vectors of length size_hash
    
    
    
    int tag, data, dest;
    
    MPI_Recv(&data, 1, MPI_INT, dest, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    while (tag != 0){
    
        if (tag == 4){
        
        
            if (flag ==0){
                MPI_Send(&hash[data], 1, MPI_INT, dest, 4, MPI_COMM_WORLD);
            }//ABC
            
            else if (flag==1){
                MPI_Send(&hash[data], 1, MPI_INT, dest, 4, MPI_COMM_WORLD);
            }//SAX
            
            else if (flag==2){
                for (int i = 0; i < size_hash; i++){
                    MPI_Send(&vectors[data][i], 1, MPI_FLOAT, dest, 4, MPI_COMM_WORLD);
                }//for sending vector
            }//SSH
        
        }//if tag 4 for hash function
        //send the hash function when requested
        //request contains index of the hash function requested
        //one element if ABC or SAX but size_hash elements if SSH
        //send back to requester with tag 4
        
        else if (tag == 6){
        
            //send the list of breakpoints
            for (int i = 0; i < num_symbols; i++){
            
                MPI_Send(&breakpoints[i], 1, MPI_INT, dest, 6, MPI_COMM_WORLD);
        
            }//for sending breakpoints

        
        }//else for tag 6 breakpoints
        
        else if (tag == 7){
            int s2;
            MPI_Recv(&s2, 1, MPI_INT, dest, 7, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            MPI_Send(&distances[data][s2], 1, MPI_INT, dest, 7, MPI_COMM_WORLD);
        
        
        }//else for tag 7 distances
        
        //to start next cycle properly
        MPI_Recv(&data, 1, MPI_INT, dest, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
    }//while tag is not 0


}

/******* HASH TABLES *********/
//keeps a dictionary of the hash buckets for a specific hash function
//receives a data item index, with its main worker index and its hash code for this hash function
//INPUTS: size_hash
//INCOMING:
//OUTGOING:
//tag 0 to shut down
//tag 1 is sim pair
void hashtable_fn(size){
    //some structure to hold the hashtable
    table
    
    int data, tag;
    int item;
    int primary;
    int dest;
    int count;
    
    //single hash code 
    //char[] hash
    
    MPI_Recv(&data, 1, MPI_INT, dest, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    while(tag != 0){
    

        item = data;
        
        //receive the data items primary worker rank
        MPI_Recv(&primary, 1, MPI_INT, dest, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        //receive the hash code
        
        if (flag == 0){
        
            for (int i = 0; i < size_hash; i++){
                MPI_Recv(&hash[i], 1, MPI_INT, dest, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        }
        
        
        }//if ABC
        
        else if (flag == 1){
            for (int i = 0; i < size_hash/word_length; i++){
                MPI_Recv(&hash[i], 1, MPI_INT, dest, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        }
        
        
        
        }//else if SAX
        
        else if (flag == 2) {
        //not sure how this looks like yet
        }//else if SSH
        
        //if hash code new to table count+1
        //append(table, hash);
        //append(table[hash][0], item)
        //append(table[hash][1], primary)
        
        for (int i = 0; i < count; i++){
            MPI_Send(&table[i][0], 1, MPI_INT, size-3, 1, MPI_COMM_WORLD);
            MPI_Send(&table[i][1], 1, MPI_INT, size-3, 1, MPI_COMM_WORLD);
            MPI_Send(&item, 1, MPI_INT, size-3, 1, MPI_COMM_WORLD);
            MPI_Send(&primary, 1, MPI_INT, size-3, 1, MPI_COMM_WORLD);
        
        
        }//for sending all pairs for sim calculation
        
        

        
    
    
        //to start next cycle
        MPI_Recv(&data, 1, MPI_INT, dest, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }//while not shut down procedure
    
    
    //receive either message to shut down or data item index
    //then receive the data items hash code
    //if this hash code does not exist add to dictionary
    //add the data item index to the dictionary for its code
    //for each other data item index for this code
    //pass the indexes to sim node 
    
    //send 
    
    //send bucket
    for (int i = 0; i < count; i++){
            //send hash code
            MPI_Send(i, 1, MPI_INT, 2, 1, MPI_COMM_WORLD);
            //send this hash code bucket
            MPI_Send(&table[i][0], 1, MPI_INT, 2, 1, MPI_COMM_WORLD);
    }
    
    MPI_Send(0, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    
    //get manager message to hashtables that we are done
    //send our stuff to writer
    //once finish sending send a message to manager and shut down
    
    
}


/******* WORKER *********/
//receives assigned raw subsequence, hash function index and index of primary worker for this data item
//preprocesses the subsequence, sends to primary worker
//gets the hash function from the hash creator node
//encodes the subsequence
//sends the data item index with its primary worker index and hash code to the proper hashtable node
//INPUTS: flag, size_hash, num_symbols, word_length, average, sd
//INCOMING:
//OUTGOING:
//tag 3 is request for full data item (preprocessed)
//tag 8 is for assignment communication with the manager
//tag 0 to shut nodes down

//tag 5 to receive stream
//tag 1 for sim pair
//tag 2 for hash bucket
//tag 4 is the hash function
//tag 6 is SAX breakpoints
//tag 7 is SAX distances
//tag 9 is data item, its primary worker rank and its hash code
void worker_fn(int flag, int size_hash, int num_symbols, int word_length, float average, float, sd){

    //send message to manager that im available
    //use tag 8 
    //data = 0 if need primary
    //else data = 1
    
    //if tag is 0 then sut down
    
    //if this is not first time 
    //receive the primary index of the data item i am about to receive INT
    
    //receive index of the data item INT
    
    //receive the subsequence FLOAT
    //size_hash for ABC and SAX but elements for SSH
    
    //receive the hash index INT
    
    //need to get the proper hash index in the manager part before sending the subsequence
    //preprocess the subsequence dependent on flag
    //for ssh come up with plan to encode the minhash code
    
    //send the hash code and data item index and its primary rank to the hashtable
    

    


    

    

}

/******* MAIN *********/
//this represents one whole run of an experiment
//each parameter array starts with what experiment trial this is
//start is the zero-based index of first element to include
//elements <= number of time steps for the data items - start
//ABC parameters flag 0
// 0 start elements  num_hash size_hash step_hash 2 0 average 0 sim 
//SAX parameters flag 1
// 1 start elements num_hash size_hash step_hash num_symbols word_length average sd 0
//SSH parameters flag 2
// 2 start elements num_hash size_hash step_hash num_symbols (n for n-grams) overlap 0 0 0
//main initializes and assigns roles
int my_main(int argc, char ** argv){

	//initialize
	MPI_Init(&argc, &argv);
	
	int rank, size;
	
	int trial;
	
	int flag, start, elements;
	int num_hash, size_hash, step_hash;
	int num_symbols, word_length;
	float average, sd, sim;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	
	trial = atoi(argv[1]);
	
	flag = atoi(argv[2]);
	
	start = atoi(argv[3]);
	elements = atoi(argv[4);
	
	num_hash = atoi(argv[5]);
	size_hash = atoi(argv[6]);
	step_hash = atoi(argv[7]);
	
	num_symbols = atoi(argv[8]);
	word_length = atoi(argv[9]);

	average = atoi(argv[10]);
	sd = atoi(argv[11]);
	sim = atoi(argv[12]);
	
    //assign roles dependent on rank
	if (rank == 0) {manager_fn(flag, elements, num_hash, size_hash, step_hash, size);}
	else if (rank <= num_hash) {hashtable_fn(size);}
	else if (rank == size-1) {receiver_fn(start, elements);}
	else if (rank == size-2) {writer_fn(trial, flag);}
	else if (rank == size-3) {similarity_fn(flag, elements, num_symbols, word_length, sim, size);}
	else if (rank == size-4) {hash_fn(flag, elements, num_hash, size_hash, num_symbols, word_length);}
	else {worker_fn(flag, size_hash, num_symbols, word_length, average, sd);}

	//clean up
	//printf("rank %d done\n", rank);
	MPI_Finalize();
	return 0;
}

/*
MPI_Send(
    void* data,
    int count,
    MPI_Datatype datatype,
    int destination,
    int tag,
    MPI_Comm communicator)
MPI_Recv(
    void* data,
    int count,
    MPI_Datatype datatype,
    int source,
    int tag,
    MPI_Comm communicator,
    MPI_Status* status)
*/


/* to compile
mpicc lsh.c -o lsh
to run 
mpiexec -n  ./sigs num_elem num_sets num_hash size_hash (rest are workers)
with fgmpi (4 workers):
mpiexec -nfg 1 -n 22 ./sigs 5 8 4 4 
OR
mpiexec -nfg 22 -n 1 ./sigs 5 8 4 4
*/
