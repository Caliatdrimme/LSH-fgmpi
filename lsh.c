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
#include <math.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))


/******* FG-MPI Boilerplate begin *********/
#include "fgmpi.h"
int my_main(int argc, char **argv); /*forward declaration*/
FG_ProcessPtr_t binding_func(int argc, char **argv, int rank)
{
    return (&my_main);
}
FG_MapPtr_t map_lookup(int argc, char **argv, char *str)
{
    return (&binding_func);
}
int main(int argc, char *argv[])
{
    FGmpiexec(&argc, &argv, &map_lookup);
    return (0);
}
/******* FG-MPI Boilerplate end *********/

/******* MANAGER *********/
//receives elements from receiver
//once receives enough elements for a subsequence
//assigns worker nodes to handle this subsequence in hashing
//and sends to each the subsequence

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
void manager_fn(int elements, int num_hash, int size)
{

    printf("manager starting\n");

    printf("size is %d\n", size);

    float data;

    float *item;
    item = (float *)malloc(sizeof(float) * elements);

    //current data item index
    int count = 0;

    int worker;

    MPI_Status status;

    //use as flag for shutting down
    int shut = 0;

    while (1)
    {

        //receive this data item
        for (int j = 0; j < elements; j++)
        {
            MPI_Recv(&data, 1, MPI_FLOAT, size - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            //printf("manager received %f\n", data);
            if (status.MPI_TAG == 0)
            {
                //printf("manager received tag 0\n");
                shut = 1;
                break;
            } //shut down
            item[j] = data;
        } //for receiving the data item

        if (shut == 1)
        {
            break;
        } //shut down

        count = count + 1;

        //get message from worker that its available = data is the worker index
        MPI_Recv(&worker, 1, MPI_INT, MPI_ANY_SOURCE, 8, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        //printf("manager received worker %d is available\n", worker);

        //send the data item index to the worker
        MPI_Send(&count, 1, MPI_INT, worker, 8, MPI_COMM_WORLD);
        printf("manager assigned item %d to worker %d\n", count, worker);

        //send the subsequnce to the worker
        for (int k = 0; k < elements; k++)
        {
            MPI_Send(&item[k], 1, MPI_FLOAT, worker, 8, MPI_COMM_WORLD);

        } //for sending subsequence
        //printf("manager sent item %d\n", count);

        //printf("manager received and sent item %d\n", count);
    } //while for each data item

    //start shut down procedure

    //once all hashtables reported to be done, send message to all others to shut down
    //writer, sim, hash, storage
    //recv size-1 and hashtables 1 through num_hash shut on their own
    //manager is 0

    data = 1;
    
    
        for (int k = num_hash+1; k < size-5 ; k++)
        {
        //send MPI_INT message with tag 0 to shut down
        MPI_Send(&data, 1, MPI_INT, k, 0, MPI_COMM_WORLD);
        } //for shutting down workers
    
    
    printf("manager waiting\n");

    MPI_Recv(&data, 1, MPI_INT, size-2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);


    printf("manager shutting all else down\n");

    for (int k = 2; k <= 5; k++)
    {
        //send MPI_INT message with tag 0 to shut down
        MPI_Send(&data, 1, MPI_INT, size - k, 0, MPI_COMM_WORLD);
        //printf("sending command to %d\n", size-k);
    } //for shutting down

    free(item);

    //printf("manager done\n");

} //manager_fn

/******* RECEIVER *********/
//streams data from file element by element to manager
//manager has known node rank of 0

//tag 5 is stream
void receiver_fn(int start, int elements, char *filename)
{

    printf("receiver starting\n");

    float data;

    char str[100000];

    //open file for reading
    FILE *fp;
    fp = fopen(filename, "r");

    //printf("opened file %s\n", filename);

    int line=0;
    
    while (fgets(str, sizeof(str), fp))
    {

        int count = 0;
 
        printf("line read as string %d\n", line);
        line ++;
        
        char *ptr = str, *eptr;

        // if strtof cannot parse a float, eptr will be equal to ptr. We assume it will happen only at the end of the line
        data = strtof(ptr, &eptr);

        while (ptr != eptr) {

            if ((count >= start) && (count < start + elements))
            {

                MPI_Send(&data, 1, MPI_FLOAT, 0, 5, MPI_COMM_WORLD);
                //printf("sending %f\n", data);
                
            }
            count++;

            ptr = eptr;
            data = strtof(ptr, &eptr);
        }
    }

/*
        do
        {
            data = strtof(ptr, &eptr);
            if (ptr == eptr){
                printf("count %d\n", count);
                printf("pointer points to %d\n", (int) *eptr);
                break;}
            ptr = eptr;

            printf("%f", data);

            if ((count >= start) && (count < start + elements))
            {

                data = (float)data;

                //MPI_Send(&data, 1, MPI_FLOAT, 0, 5, MPI_COMM_WORLD);
                //printf("sending %f\n", data);
                
            }
            //printf("COUNT %d\n", count);
            count++;

            //printf("number read as float %f\n", data);
        } while ((*eptr) && (*eptr != '\n'));


        count = 0;
        */
        

    MPI_Send(&data, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD);
    //printf("receiver sent %f to manager with tag 0\n", data);

    fclose(fp);

    //printf("receiver done\n");
} //receiver_fn

/******* WRITER *********/
//writes results sent to it to file
//location and name of file hardcoded here
//form: index of data item 1, index of data item 2, similarity
//or at end of all processes:
//hash index, hash code, list of all data item indexes

//tag 0 is shut down
//tag 1 is sim pair
//tag 2 is hash bucket
void writer_fn(int trial, int flag, int size_hash, int num_hash)
{
    //printf("writer starting\n");
    //create results file
    FILE *fp;

    char name[10];

    char append[2];

    if (flag == 0)
    {
        //ABC
        strcat(name, "ABC");
    }
    else if (flag == 1)
    {
        //SAX
        strcat(name, "SAX");
    }
    else if (flag == 2)
    {
        //SSH
        strcat(name, "SSH");
    }

    //printf("%s\n",name);

    sprintf(append, "%d", trial); // put the int into a string
    strcat(name, append);

    strcat(name, ".txt");

    fp = fopen(name, "w");
    
    fprintf(fp, "hash code size is %d\n", size_hash);

    MPI_Status status;

    int data;
    
    int done = 0;

    while (1)
    {

        MPI_Recv(&data, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        //printf("recieved %d\n", data);

        if (status.MPI_TAG == 0)
        {
            done++;
            if (done == num_hash+1){break;}
            continue;
        } //if tag is 0 break

        else if (status.MPI_TAG == 1)
        {
        //printf("else clause\n");
            int ind2;
            float sim;
            fprintf(fp, "sim ");
            fprintf(fp, "%d ", data);
            MPI_Recv(&ind2, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("recieved %d\n", ind2);
            fprintf(fp, "%d ", ind2);
            MPI_Recv(&sim, 1, MPI_FLOAT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            fprintf(fp, "%f", sim);
            //printf("recieved %f\n", sim);

        } //if tag is 1 receive sim

        else if (status.MPI_TAG == 2)
        {
        
        //printf("else if clause\n");
            int ind, size, elem;

            fprintf(fp, "bucket ");
            //printf("receiving bucket\n");

            MPI_Recv(&ind, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            fprintf(fp, "%d ", ind);

            //MPI_Recv(&size, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            MPI_Recv(&elem, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            fprintf(fp, "%d", elem);


            for (int i = 0; i < data; i++)
            {
                MPI_Recv(&elem, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                fprintf(fp, " %d", elem);
                

            } //for hash bucket
            
            //printf("received bucket\n");

        } //if tag is 2 receive hash bucket

        //print new line after each message written
        fprintf(fp, "%s", "\n");

    } //while
    
    MPI_Send(&data, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

    fclose(fp);

    //printf("writer done\n");
} //writer_fn

/******* SIMILARITY *********/

//calculates the similarity by ABC
float ABC_sim(int *item1, int *item2, float sim, int elements)
{

    int c = 0;
    float similarity = 0;
    for (int i = 0; i < elements; i++)
    {
        if (item1[i] == item2[i])
        {
            similarity = similarity + pow((1 + sim), c);
            c = c + 1;
        }
        else
        {
            c = 0;
        }
    }
    return similarity;

} //ABC_sim

//calculates the similarity by SAX
//n is total elements, w is number of words after preprocessing
float SAX_sim(int *item1, int *item2, int n, int w, int size, int rank)
{

    float one = sqrt(n / w);
    
    //printf("n is %d and w is %d\n", n, w);

    //printf("one is %f\n", one);

    float sum = 0;
    for (int i = 0; i < w; i++)
    {

        int ind1 = item1[i];

        int ind2 = item2[i];

        MPI_Send(&ind1, 1, MPI_INT, size - 4, 7, MPI_COMM_WORLD);

        MPI_Send(&ind2, 1, MPI_INT, size - 4, 7, MPI_COMM_WORLD);
        
        //printf("sent\n");

        float dist;

        MPI_Recv(&dist, 1, MPI_FLOAT, size - 4, 7, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        sum = sum + pow(dist, 2);
        
        //printf("sum is %f dist is %f for symbols %d %d\n", sum, dist, ind1, ind2);
    }

    float two = sqrt(sum);
    
    //printf("two is %f\n", two);

    return one * two;

} //SAX_sim

//calculates the similarity by SSH
float SSH_sim(int *item1, int *item2, int size)
{

    float res = 0.0;
    float total = 0.0;

    for (int i = 0; i < size; i++)
    {

        if (item1[i] > 0)
        {
            if (item2[i] > 0)
            {

                res = res + 1.0;
            }
            total = total + 1.0;
        }

        if (item2[i] > 0)
        {
            if (item1[i] == 0)
            {
                total = total + 1;
            }
        }
    }
    return (float)res / total;

} //SSH_sim

//recieves two indexes of data items
//gets the data items and calculates the similarity
//sends info along to writer

//tag 0 is shut down
//tag 1 is sim pair
void similarity_fn(int flag, int elements, int num_symbols, int word_length, float siml, int size, int rank, int num_hash)
{

    //printf("similarity starting\n");

    int data;

    MPI_Status status;

    int elems;
    if (flag == 1)
    {
        elems = elements / word_length;
        //printf("elems is %d\n", elems);
        //printf("word length is %d\n", word_length);

    } //lower length of preprocessed data item if SAX

    else if (flag == 2)
    {
        elems = pow(2, num_symbols);
    } //else if  SSH

    else
    {
        elems = elements;
    } //else if ABC

    //malloc item1
    int *item1;
    item1 = (int *)malloc(sizeof(int) * elems);

    //malloc item2
    int *item2;
    item2 = (int *)malloc(sizeof(int) * elems);
    
    int done = 0;

    while (1)
    {

        MPI_Recv(&data, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
       //printf("SIM recieved message with tag %d from node %d\n", status.MPI_TAG, status.MPI_SOURCE);

        if (status.MPI_TAG == 0)
        {
            done++;
            if (done == num_hash){break;}
            continue;
        } //if tag is 0 break

        int ind1, ind2, rank1, rank2;

        //save first index
        ind1 = data;

        //receive second index
        //printf("SIM recv first index %d\n", ind1);

        MPI_Recv(&ind2, 1, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        //printf("SIM recv second index %d\n", ind2);
        //get item1

        //send request to size-5 storage node
        MPI_Send(&ind1, 1, MPI_INT, size - 5, 2, MPI_COMM_WORLD);
        
        
        //receive elements elements with tag 2 and store as item1
        for (int i = 0; i < elems; i++)
        {
    
            MPI_Recv(&item1[i], 1, MPI_INT, size - 5, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        } //for getting item1

        //get item2

        MPI_Send(&ind2, 1, MPI_INT, size - 5, 2, MPI_COMM_WORLD);
        //printf("receiving item 2\n");
        //receive elements elements with tag 2 and store as item2
        for (int i = 0; i < elems; i++)
        {
            MPI_Recv(&item2[i], 1, MPI_INT, size - 5, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        } //for getting item2

    

        //calc the sim
        float simf=0;
        if (flag == 0)
        {
            //ABC
            simf = ABC_sim(item1, item2, siml, elements);
            //printf("sim for items %d and %d is %f\n", ind1, ind2, simf);
        }
        else if (flag == 1)
        {
            //SAX
            simf = SAX_sim(item1, item2, elements, elems, size, rank);
        }
        else if (flag == 2)
        {
            //SSH
        //printf("calc sim\n");
            simf = SSH_sim(item1, item2, elems);
            //printf("sim calc done\n");
        }

        //send the sim pair to writer rank size-2 with tag 1
        MPI_Send(&ind1, 1, MPI_INT, size - 2, 1, MPI_COMM_WORLD);
        MPI_Send(&ind2, 1, MPI_INT, size - 2, 1, MPI_COMM_WORLD);
        MPI_Send(&simf, 1, MPI_FLOAT, size - 2, 1, MPI_COMM_WORLD);
        printf("sent %d %d %f pair to writer\n", ind1, ind2, simf);

    } //while tag is not 0

    free(item1);
    free(item2);
    
    data = 0;
    
    MPI_Send(&data, 1, MPI_INT, size-2, 0, MPI_COMM_WORLD);

    //printf("similarity done\n");
} //sim_fn

/******* HASH *********/

int cmpfunc(const void *a, const void *b)
{
    return (*(int *)a - *(int *)b);
}

int *random_indexes(int n, int elements, int size_hash)
{

    srand(time(NULL));

    //return array of size n
    //holds random indexes between 0 and elements

    int *indexes;
    indexes = (int *)malloc(sizeof(int) * n);

    int max_elem = elements - size_hash + 1;

    int *elem;
    elem = (int *)malloc(sizeof(int) * max_elem);

    for (int i = 0; i < max_elem; i++)
        elem[i] = 0;

    // Random permutation the order
    for (int i = 0; i < n; i++)
    {
        int j;

        do
        {
            j = rand() % max_elem;
        } while (elem[j] == 1);

        indexes[i] = j;
        elem[j] = 1;
    } //for generating random indexes
    //printf("end loop");
    //sort in ascending order

    qsort(indexes, n, sizeof(indexes[0]), cmpfunc);

    free(elem);
    return indexes;

} //random_indexes

float *create_random_matrix(int m, int n, int k)
{

    //return array of size n
    //holds random indexes between 0 and elements

    //over all rows
    srand(time(NULL));
    float *res;
    res = (float *)malloc(sizeof(float) * m * n);

    for (int j = 0; j < m; j++)
    {
        for (int i = 0; i < n; i++)
        {

            *(res + j * m + i) = k * ((float)rand() / (float)RAND_MAX);

            //printf("row %d column %d value %f\n", j, i, *(res + j*m + i));

        } //for generating random indexes for one row
    }

    return res;
} //create_random_matrix

float *random_vector(int size)
{
    //return a random vector of size
    srand(time(NULL));

    float *vector;
    vector = (float *)malloc(sizeof(float) * size);

    for (int j = 0; j < size; j++)
    {

        vector[j] = (float)rand() / (float)RAND_MAX;

    } //for elements

    return vector;

} //random_vector

float *create_breakpoints(int num_symbols)
{

    //from 3 to 10 symbols
    float breakpoints[8][9] = {{-0.43, 0.43, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0},
                               {-0.67, 0, 0.67, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0},
                               {-0.84, -0.25, 0.25, 0.84, 0.0, 0.0, 0.0, 0.0, 0.0},
                               {-0.97, -0.43, 0, 0.43, 0.97, 0.0, 0.0, 0.0, 0.0},
                               {-1.07, -0.57, -0.18, 0.18, 0.57, 1.07, 0.0, 0.0, 0.0},
                               {-1.15, -0.67, -0.32, 0, 0.32, 0.67, 1.15, 0.0, 0.0},
                               {-1.22, -0.76, -0.43, -0.14, 0.14, 0.43, 0.76, 1.22, 0.0},
                               {-1.28, -0.84, -0.52, -0.25, 0, 0.25, 0.52, 0.84, 1.28}};

    //return array of size num_symbols-1
    //holds breakpoints as values
    //to have equal areas under normal curve

    float *breakpoints_sent;
    breakpoints_sent = (float *)malloc(sizeof(float) * num_symbols - 1);

    for (int i = 0; i < num_symbols - 1; i++)
    {
        breakpoints_sent[i] = breakpoints[num_symbols - 3][i];
    }

    return breakpoints_sent;

} //create_breakpoints

float *create_distances(int num_symbols, float *breakpoints)
{
    //return num_symbolsXnum_symbols array that holds distances between symbols
    //under the normal curve

    float *distances = (float *)malloc(num_symbols * num_symbols * sizeof(float));

    for (int i = 0; i < num_symbols; i++)
    {
        for (int j = 0; j < num_symbols; j++)
        {
            if ((abs(i - j)) <= 1)
            {
                *(distances + i * num_symbols + j) = 0;
            } //if

            else
            {

                int mx = MAX(i, j) - 1;
                int mn = MIN(i, j);

                *(distances + i * num_symbols + j) = breakpoints[mx] - breakpoints[mn];

            } //else
        }
    }

    return distances;

} //create_distances

//creates the hash functions
//use tag 4 for hash function
//tag 0 is shut down
//tag 6 for permutations
void hash_fn_SSH(int num_hash, int size_hash, int num_symbols)
{

    //create the necessary hash functions
    //for SSH it is a random vector of length size_hash
    float *hash = random_vector(size_hash);

    //create perm num_hash X l floats random from 0 to 2**num_symbols

    int k = pow(2, num_symbols);

    int l = k / 2;

    float *perm = create_random_matrix(num_hash, l, k);

    MPI_Status status;

    int dest;

    //MPI_Recv(&dest, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    while (1)
    {

        MPI_Recv(&dest, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == 0)
        {
            break;
        }

        if (status.MPI_TAG == 4)
        {
        
            for (int i = 0; i < size_hash; i++)
            {
             
                MPI_Send(&hash[i], 1, MPI_FLOAT, dest, 4, MPI_COMM_WORLD);
            }
            
            //for sending hash vector
        }
        else if (status.MPI_TAG == 6)
        {

            for (int i = 0; i < num_hash; i++)
            {
            
            
                 for (int j = 0; j < l; j++)
                {
                    MPI_Send(&perm[i], 1, MPI_FLOAT, dest, 6, MPI_COMM_WORLD);
                    //printf("sending to %d\n", dest);
                    
                    }
                //*(perm + i*num_hash + j)) j til l
            } //for sending hash permutations
        }

    } //while tag is not 0

    free(hash);
    free(perm);

} //hash SSH

void hash_fn_SAX(int elements, int num_hash, int num_symbols, int word_length, int size_hash)
{

    //create the necessary hash functions
    //for SAX it is num_hash indexes between 0 and elements/word_length

    //for SAX need to create table of breakpoints tag 6
    //and precalculate distances tag 7

    int *hash = random_indexes(num_hash, elements / word_length, size_hash);

    float *breakpoints = create_breakpoints(num_symbols);

    float *distances = create_distances(num_symbols, breakpoints);

    int data, dest;

    MPI_Status status;

    while (1)
    {

        MPI_Recv(&dest, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == 0)
        {
            break;
        }

        if (status.MPI_TAG == 4)
        {

            
            //for sending vector
            
            for (int i = 0; i < num_hash; i++)
            {
                //printf("hash value %d\n", hash[i]);
                MPI_Send(&hash[i], 1, MPI_INT, dest, 4, MPI_COMM_WORLD);
            }

        } //if tag 4 for hash function

        else if (status.MPI_TAG == 6)
        {

            //send the list of breakpoints
            
            for (int i = 0; i < num_symbols; i++)
            {
                //printf("hash value %d\n", hash[i]);
                MPI_Send(&breakpoints[i], 1, MPI_FLOAT, dest, 6, MPI_COMM_WORLD);
            }


            //for sending breakpoints

        } //else for tag 6 breakpoints

        else if (status.MPI_TAG == 7)
        {
            int s1, s2;
               
            //MPI_Recv(&s1, 1, MPI_INT, MPI_ANY_SOURCE, 7, MPI_COMM_WORLD, &status);
            MPI_Recv(&s2, 1, MPI_INT, status.MPI_SOURCE, 7, MPI_COMM_WORLD, &status);

            MPI_Send(&*(distances + dest * num_symbols + s2), 1, MPI_FLOAT, status.MPI_SOURCE, 7, MPI_COMM_WORLD);
            //*(distances + i*num_symbols + j); this works for print i row j column both num_symbols

        } //else for tag 7 distances

    } //while tag is not 0
    free(breakpoints);
    free(distances);
    free(hash);

} //hash SAX

void hash_fn_ABC(int elements, int num_hash, int size_hash)
{

    //create the necessary hash functions
    //for ABC it is num_hash indexes between start and start+elements-1

    //printf("ABC hash starting\n");

    int *hash = random_indexes(num_hash, elements, size_hash);

    //printf("ABC hash saved\n");

    int dest;

    MPI_Status status;

    while (1)
    {

        MPI_Recv(&dest, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        //printf("hash requested from node %d\n", dest);

        if (status.MPI_TAG == 0)
        {
            break;
        }

        if (status.MPI_TAG == 4)
        {

            for (int i = 0; i < num_hash; i++)
            {
                //printf("hash value %d\n", hash[i]);
                MPI_Send(&hash[i], 1, MPI_INT, dest, 4, MPI_COMM_WORLD);
            }
            //printf("hash sent\n");
            //for sending vector

        } //if tag 4 for hash function

    } //while tag is not 0

    free(hash);

    //printf("ABC hash done\n");

} //hash ABC

/******* HASH TABLES *********/

//int n is total number of hash codes with their buckets stored so far
//counts 1d int array of size n
//codes 2d int array - each row is of size_hash size , n rows
//items 2d int array - each row i is of size counts[i], n rows

//index is the hash code index to whose bucket we added the item

int *resize_table(int *array, int size)
{

    int *new;
    new = (int *)malloc(sizeof(int) * size + 1);

    for (int i = 0; i <= size; i++)
    {
        new[i] = array[i];
    } //copy all old items

    free(array);

    return new;

} //resize

typedef struct Hashtable
{
    int n;
    int *codes;
    int *counts;
    int *items;
    int index;
    int c;
} hashtable;

//reads the array as a decimal int
int to_dec(int *data, int num_symbols)
{

    int res = 0;

    int unit = 1;

    for (int i = num_symbols - 1; i >= 0; i--)
    {

        res = res + unit * data[i];

        unit = unit * 10;
    }
    return res;

} //to decimal index

//has to return n, codes, counts, items, index
struct Hashtable append_to_table(struct Hashtable table, int hash, int item, int rank)
{
    //check if the hash code exists in the hash codes table
    //if not resize all the tables
    int ind = table.n;

    for (int i = 0; i < table.n; i++)
    {
        if (table.codes[i] == hash)
        {
            ind = i;
            break;
        } //if found
    }     //for searching hash code

    if (ind == table.n)
    {
        table.n = table.n + 1;
        table.codes = resize_table(table.codes, table.n);
        table.counts = resize_table(table.counts, table.n);
        table.counts[ind] = 0;

        table.codes[ind] = hash;

    int c = table.counts[ind];
    //printf("index %d and count %d\n", ind, c);

        int s = 0;

        for (int i = 0; i < table.n -1; i++)
        {
            // printf("here! table.index=%d\n", table.index);
            s = s + table.counts[i];
        } //for calculating size of items array

        table.items = resize_table(table.items, s);

        table.c = s;
        //*(table.items + s) = item;
        //printf("hashtable inserted item %d\n", item);
        // printf("but not here\n");
    } //if didnt find it need to resize

    else
    {
        int s = 0;

        for (int i = 0; i < table.n; i++)
        {
            s = s + table.counts[i];
        } //for calculating size of items array

        table.items = resize_table(table.items, s);

        int t = 0;

        for (int i = 0; i <= ind; i++)
        {
            t = t + table.counts[i];
        } //for calculating size of items array before the newly inserted item

        //move all items after it
        for (int j = s; j > t; j--)
        {
            table.items[j] = table.items[j - 1];
        }
        //insert item
        table.c = t;
        //table.items[t] = item;
        //printf("hashtable inserted item %d\n", item);
    }

    table.items[table.c] = item;
    //printf("hashtable inserted item %d\n", item);
     //printf("old table.index to %d\n", table.counts[ind]);
    table.counts[ind] = table.counts[ind] + 1;
    //printf("updated table.index to %d\n", table.counts[ind]);
    //insert the hash code, item  at the right index, update count
    table.index = ind;
    
    int t = 0;

    for (int i = 0; i < ind; i++)
        {
            t = t + table.counts[i];
        } //for calculating size of items array before the newly inserted item
    
    for (int i =0; i < table.counts[ind]; i++){
        //printf("bucket %d has item %d for hash %d and code %d\n", ind, table.items[t+i], rank, table.codes[ind]);
    }
    
    
    return table;
} //append to table

//keeps a dictionary of the hash buckets for a specific hash function
//receives a data item index, and its hash code for this hash function

//tag 0 to shut down
//tag 1 is sim pair
void hashtable_fn(int size, int size_hash, int rank, int flag, int num_workers, int n)
{
    //printf("hashtable %d starting\n", rank);
    int data;
    int item;
    int count;

    int *hash;
    hash = (int *)malloc(sizeof(int) * size_hash);

    int hash_SSH;

    struct Hashtable table;

    table.n = 0;

    
    table.codes = (int *)malloc(sizeof(int));

   
    table.items = (int *)malloc(sizeof(int));

    table.counts = (int *)malloc(sizeof(int));


    table.index = 0;
    table.c = 0;

    int nbor;
    MPI_Status status;

    int done = 0;

    int t;

    int added = 0;

    while (1)
    {
        // printf("entered\n");
        MPI_Recv(&data, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == 0)
        {
        done++;

            if (done == num_workers)
            {
            //printf("hashtable shutting down\n");
                break;
            }
            //printf("hashtable %d got shut down command %d from %d\n", rank, done, status.MPI_SOURCE);
            continue;
        }

        added = 1;

        item = data;

        //receive the hash code

        //printf("hashtable %d got item %d from worker %d\n", rank, item, status.MPI_SOURCE);

        if (flag == 0)
        {
            for (int i = 0; i < size_hash; i++)
            {
                MPI_Recv(&hash[i], 1, MPI_INT, status.MPI_SOURCE, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        } //if ABC

        else if (flag == 1)
        {
            for (int i = 0; i < size_hash; i++)
            {
                MPI_Recv(&hash[i], 1, MPI_INT, status.MPI_SOURCE, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        } //else if SAX

        else if (flag == 2)
        {
            MPI_Recv(&hash_SSH, 1, MPI_INT, status.MPI_SOURCE, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("hashtable received hash value\n");
        } //else if SSH

        if (flag == 2)
        {
            // printf("if\n");
            table = append_to_table(table, hash_SSH, item, rank);
            // printf("ifdone\n");
        }
        else
        {
            // printf("else\n");
            table = append_to_table(table, to_dec(hash, size_hash), item, rank);
            // printf("elsedone\n");
        }
            //printf("size of bucket is %d\n", table.counts[table.index]);
        // printf("something\n");
        if (table.counts[table.index] > 1)
        {
            //printf("size of bucket is %d\n", table.counts[table.index]);

            int k = table.c - table.counts[table.index] +1;

            for (int i = k; i < table.counts[table.index] - 1; i++)
            {
                //item was inserted at table.items[table.c]
                //neighbours are in table.items starting from table.c-table.counts[table.index]+1
                nbor = table.items[i];
                printf("hashtable %d sending pair %d %d\n", rank, nbor, item);
                MPI_Send(&nbor, 1, MPI_INT, size - 3, 1, MPI_COMM_WORLD);
                MPI_Send(&item, 1, MPI_INT, size - 3, 1, MPI_COMM_WORLD);

            } //for sending all pairs for sim calculation
        }     //if have items to send
        //printf("hashtable %d done with item %d\n", rank, item);
    } //while not shut down procedure

    //receive either message to shut down or data item index
    //then receive the data items hash code
    //if this hash code does not exist add to dictionary
    //add the data item index to the dictionary for its code
    //for each other data item index for this code
    //pass the indexes to sim node

    //send

    //send bucket
    for (int i = 0; i < table.n; i++)
    {
    
        printf("hashtable %d sending bucket %d\n", rank, i);
        //size of bucket
        MPI_Send(&table.counts[i], 1, MPI_INT, size-2, 2, MPI_COMM_WORLD);
        //hash index
        MPI_Send(&rank, 1, MPI_INT, size-2, 2, MPI_COMM_WORLD);
        //size of hash code
        //MPI_Send(&code_size, 1, MPI_INT, size-2, 2, MPI_COMM_WORLD);
        //send hash code
        MPI_Send(&table.codes[i], 1, MPI_INT, size-2, 2, MPI_COMM_WORLD);
        //send this hash code bucket
        //MPI_Send(&table.items[i], table.counts[i], MPI_INT, size-2, 2, MPI_COMM_WORLD);

        t = 0;

        for (int k = 0; k < i; k++)
        {
            t = t + table.counts[k];
        }

        for (int j = 0; j < table.counts[i]; j++)
        {
            int data = table.items[t+j];
            
            //*(table.items + i * t + j);
            MPI_Send(&data, 1, MPI_INT, size-2, 2, MPI_COMM_WORLD);
        }
    }
    //printf("hashtable %d sending done command to writer\n", rank);
    MPI_Send(&rank, 1, MPI_INT, size-2, 0, MPI_COMM_WORLD);
    MPI_Send(&rank, 1, MPI_INT, size-3, 0, MPI_COMM_WORLD);

    printf("Hashtable %d sent\n", rank);

    //get manager message to hashtables that we are done
    //send our stuff to writer
    //once finish sending send a message to manager and shut down

    free(hash);

    free(table.counts);
    free(table.items);
    free(table.codes);

    printf("Hashtable %d done\n", rank);

} //hashtable fn

/*************STORAGE*****************/
void storage_fn(int n, int elements)
{

    //printf("storage starting\n");
    // float table[n][elements];
    int *table = (int *)malloc(n * elements * sizeof(int));
    int index;

    /*
    for (int i =0; i < elements; i++){
        *(table + (ind1-1)*elements + i) = data1[i];
    
    }
    
    for (int i = 0; i < n; i++)
        for (int j = 0; j < elements; j++){
        
            printf("item saved at index %d %d is %f\n", i, j, *(table + j + i*elements));
        }
        
    
    }
    
    */
    int item;

    MPI_Status status;
    while (1)
    {
        MPI_Recv(&index, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status); // get row of table
        //printf("STORAGE received message from %d with tag %d\n", status.MPI_SOURCE, status.MPI_TAG);
        if (status.MPI_TAG == 0)
        {
            //printf("storage got shut down command from %d\n", status.MPI_SOURCE);
            break;
        }
        if (status.MPI_TAG == 3)
        {

            for (int i = 0; i < elements; i++)
            {
                MPI_Recv(&item, 1, MPI_INT, status.MPI_SOURCE, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                *(table + (index - 1) * elements + i) = item;
                //printf("saved element %d in index %d for item %d\n", item, (index - 1)*elements +i, index);
            }
            //printf("received item %d for storage\n", index);
        } //receiveing
        if (status.MPI_TAG == 2)
        {
        
        
            for (int i = 0; i < elements; i++)
            {
                item = *(table + (index - 1) * elements + i);
                
                MPI_Send(&item, 1, MPI_INT, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
            }
            //printf("storage sent item %d to node %d\n", index, status.MPI_SOURCE);
        } //sending
    }     //while

    free(table);
    //printf("storage done\n");
} //storage

/******* WORKER *********/

float *normalize(float *stored, int size, float average, float sd)
{

    float *res;
    res = (float *)malloc(sizeof(float) * size);

    for (int i = 0; i < size; i++)
    {

        res[i] = (stored[i] - average) / sd;

    } //for

    return res;

} //normalize

//calculates dot product of two vectors
float dot(float *vector, float *data, int size_hash)
{

    float res = 0;

    for (int i = 0; i < size_hash; i++)
    {

        res = res + vector[i] * data[i];
    }

    return res;

} //dot product

//turns float data vector to a smaller binary vector representing the signs of the
//dot product projections of subsequences with the hash vector
int *sketch(float *data, float *vector, int elements, int step_hash, int size_hash, int size_sketched)
{

    int *res;
    res = (int *)malloc(sizeof(int) * size_sketched);

    float *sub;
    sub = (float *)malloc(sizeof(float) * size_hash);

    int ind = 0;

    for (int i = 0; i < size_sketched; i++)
    {

        for (int j = 0; j < size_hash; j++)
        {
            sub[j] = data[ind + j];
        }

        if (dot(vector, sub, size_hash) >= 0)
        {
            res[i] = 1;
        } //if
        else
        {
            res[i] = 0;
        }

        ind = ind + step_hash;
    }

    free(sub);
    return res;

} //sketch

//reads the array as a binary number
int to_bin(int *data, int num_symbols)
{

    int res = 0;

    int unit = 1;

    for (int i = num_symbols - 1; i >= 0; i--)
    {

        res = res + unit * data[i];

        unit = unit * 2;
    }

    return res;

} //to binary index

//calculates number of shingles repeated in the main sketched string
//returns array of int where the ints are the counts and the shingles are the binary form of the index
int *shingle(int n_shingles, int size_shingled, int size_sketched, int num_symbols, int overlap, int *sketched)
{

    int *res;
    res = (int *)malloc(sizeof(int) * size_shingled);

    int *sub;
    sub = (int *)malloc(sizeof(int) * num_symbols);

    //fill with zeros
    for (int i = 0; i < size_shingled; i++)
    {
        res[i] = 0;
    }

    int ind = 0;

    for (int i = 0; i < n_shingles; i++)
    {

        for (int k = 0; k < num_symbols; k++)
        {
            sub[k] = sketched[ind + k];
        }

        int index = to_bin(sub, num_symbols);

        res[index]++;

        ind = ind + overlap;
    }
    free(sub);
    return res;

} //shingle

int *preprocess_ABC(float *item, int elements, float average)
{

    int *data;
    data = (int *)malloc(sizeof(int) * elements);

    for (int i = 0; i < elements; i++)
    {

        if (item[i] >= average)
        {
            data[i] = 1;
        }
        else
        {
            data[i] = 0;
        }
    }

    return data;
} //preprocess_ABC

int *preprocess_SAX(float *item, int elements, int num_symbols, int size, int rank, int word_length)
{

    int w = elements / word_length;
    float *ave;
    ave = (float *)malloc(sizeof(float) * w);

    float sum = 0;
    int count = 0;

    int ind = 0;

    for (int i = 0; i < w - 1; i++)
    {
        //create substring
        //find average
        for (int j = 0; j < word_length; j++)
        {
            sum = sum + item[ind + j];
        }
        ave[i] = sum / word_length;
        ind = ind + word_length;
        sum = 0;
    }

    for (int i = ind; i < elements; i++)
    {
        sum = sum + item[i];
        count++;
    }

    ave[w - 1] = sum / count;

    //malloc breakpoints

    float *breakpoints;
    breakpoints = (float *)malloc(sizeof(float) * num_symbols - 1);

    int data = 1;

    MPI_Send(&rank, 1, MPI_INT, size - 4, 6, MPI_COMM_WORLD);

    for (int i = 0; i < num_symbols - 1; i++)
    {

        MPI_Recv(&breakpoints[i], 1, MPI_FLOAT, size - 4, 6, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    
    //printf("received breakpoints\n");

    //malloc int[] data of size elements

    int *res;
    res = (int *)malloc(sizeof(int) * w);
    
    //printf("the number of words is %d\n", w);

    for (int k = 0; k < w; k++)
    {
        res[k] = num_symbols - 1;
        //printf("loop\n");
    }
    
    //printf("the number of words is %d\n", w);

    for (int i = 0; i < w; i++)
    {
        //find the symbol
        for (int j = 0; j < num_symbols - 1; j++)
        {
            if (breakpoints[j] >= ave[i])
            {
                res[i] = j;
                break;
            }
        }
    }

    free(breakpoints);
    free(ave);

    return res;

} //preprocess_SAX

void preprocess_SSH(int rank, int size, int index, float *data, int elements, int num_hash, int step_hash, int size_hash, int num_symbols, int overlap, float *hash_matrix, float *vector)
{

    //INPUT float array of size elements - data
    //size - total number of nodes
    //index - index of the item being processed
    //int rank, size, index, data, elements, num_hash, step_hash, size_hash, num_symbols, overlap

    //preprocess
    //result of this needs to be saved calculate distance later
    //this is an array of int - 0 or 1 - index in binary encoding is the shingle

    //SKETCH
    //float array subsequenced into overlapping windows and dot product with vector
    //INPUT float array size elements
    //OUTPUT int (0 or 1) array size size_sketched
    //can use step_hash (moving of the vector across string) and size_hash (vector length)
    int size_sketched = (elements - size_hash) / step_hash + 1;

    int *sketched = sketch(data, vector, elements, step_hash, size_hash, size_sketched);

    //SHINGLE
    //INPUT binary array size size_sketched
    //OUTPUT int array size size_shingled
    //each elements index in binary is the actual shingle
    //number is number of this shingle occuring
    //num_symbols is length of each shingle, overlap is overlap for shingling
    int size_shingled = pow(2, num_symbols);

    int n_shingles = (size_sketched - num_symbols) / overlap + 1;

    int *shingled = shingle(n_shingles, size_shingled, size_sketched, num_symbols, overlap, sketched);

    //save the shingled version to storage
    MPI_Send(&index, 1, MPI_INT, size - 5, 3, MPI_COMM_WORLD);
    
   for (int i = 0; i < size_shingled; i++)
       {
                
          MPI_Send(&shingled[i], 1, MPI_INT, size-5, 3, MPI_COMM_WORLD);
       }

    printf("SSH sent to storage\n");

    //sending the shingled version to storage - tag 3 rank size-5

    //https://papers.nips.cc/paper/2016/file/c2626d850c80ea07e7511bbae4c76f4b-Paper.pdf
    //HASH
    //get the hash code
    //send hash code and item index to hashtable

    //create m hash permutations
    //each hash code is an integer - represents first one found that is in the green for this item

    //get a length for each hash somehow?  have 2**k shingles to check - want to have a quarter of them for each permutation?

    //create mXn matrix
    //follow the permutation
    //check if the indexes' data for this item is non-zero
    //check if its in the green
    //stop if it is - this is the hash code
    //repeat m times;

    int l = size_shingled / 2;
    
    //printf("L is %d\n", l);

    for (int i = 0; i < num_hash; i++)
    {
    //printf("I is %d\n", i);
    

        for (int j = 0; j < l; j++)
        {
        //printf("J is %d\n", j);
            int ind = floor(*(hash_matrix + i * l + j));
            
            //printf("IND is %d\n", ind);

            if (*(hash_matrix + i * l + j) - ind < (float)shingled[ind] / (float)n_shingles)
            {
                MPI_Send(&index, 1, MPI_INT, i, 9, MPI_COMM_WORLD);
                //send the hash code j and item index to appropriate hashtable node i
                MPI_Send(&j, 1, MPI_INT, i, 9, MPI_COMM_WORLD);
                
                //printf("ssh for %d sending hash value %d for hash %d\n", rank, j, i);
                //if did not find the hash - not sending anything
                break;
            }
        }

    } //for each hash

    free(sketched);
    free(shingled);

} //preprocess_SSH

//receives assigned raw subsequence
//preprocesses the subsequence, sends to storage
//gets the hash function from the hash creator node
//encodes the subsequence
//sends the data item index and hash code to the proper hashtable node

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
void worker_fn(int rank, int flag, int size_hash, int step_hash, int num_symbols, int word_length, float average, float sd, int size, int elements, int num_hash)
{

    printf("worker %d starting\n", rank);

    float *stored;
    stored = (float *)malloc(sizeof(float) * elements);

    int item_index;

    int length = elements / word_length;

    //int[elements] ABC;
    int *ABC;
    ABC = (int *)malloc(sizeof(int) * elements);

    //int[length] SAX;
    int *SAX;
    SAX = (int *)malloc(sizeof(int) * length);

    //initilize code vectors

    //int[size_hash] code
    int *code;
    code = (int *)malloc(sizeof(int) * size_hash);
    //what is the size of the SSH code?

    int *hash;
    hash = (int *)malloc(sizeof(int) * num_hash);

    //printf("flag is %d\n", flag);
    


    if (flag != 2)
    {
        //send request to hash node
        //printf("size - 4 is %d\n", size-4);
        MPI_Send(&rank, 1, MPI_INT, size - 4, 4, MPI_COMM_WORLD);
        //MPI_Recv(&hash, num_hash, MPI_INT, size-4, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        //printf("worker %d requested hash\n",rank);
        for (int i = 0; i < num_hash; i++)
        {
            MPI_Recv(&hash[i], 1, MPI_INT, size - 4, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("hash value %d is %d for worker %d\n", i, hash[i], rank);
        }
    }
    
     //printf("WORKER\n");

    int l = pow(2, num_symbols) / 2;

    float *hash_matrix;
    hash_matrix = (float *)malloc(sizeof(float) * num_hash * l);

    if (flag == 2)
    {
        //send request hash  tag 6
        MPI_Send(&rank, 1, MPI_INT, size - 4, 6, MPI_COMM_WORLD);
        //receive hash array tag 6
        
        
        for (int i = 0; i < num_hash; i++)
            {
            
             for (int j = 0; j < l; j++)
                {
                    //printf("RECV %d %d %d\n", rank, i, j);
                 MPI_Recv(&hash_matrix[i*l + j], 1, MPI_FLOAT, size - 4, 6, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
    }
    //*(hash_matrix + i* l + j)
    
    //printf("WORKER %d here \n", rank);
    //malloc float[size_hash] vector;//for SSH
    float *vector;
    vector = (float *)malloc(sizeof(float) * size_hash);

    if (flag == 2)
    {
        //request and save vector
        MPI_Send(&rank, 1, MPI_INT, size - 4, 4, MPI_COMM_WORLD);
        //get hash vector from the hash node
        for (int i = 0; i < size_hash; i++)
        {
            MPI_Recv(&vector[i], 1, MPI_FLOAT, size - 4, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } //for receiving vector
    }

    MPI_Status status;

    while (1)
    {
        //printf("WORKER %d sending...\n", rank);
        MPI_Send(&rank, 1, MPI_INT, 0, 8, MPI_COMM_WORLD);
        printf("worker %d requested data\n",rank);
        //send message to manager that im available
        //use tag 8

        //if tag is 0 then shut down
        MPI_Recv(&item_index, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        //printf("WORKER %d received %d from %d with tag %d\n", rank, item_index, status.MPI_SOURCE, status.MPI_TAG);

        if (status.MPI_TAG == 0)
        {
            //printf("worker %d breaking \n", rank);
            break;
        } //if shut down
        
        //printf("worker %d processing item %d\n", rank, item_index);

        for (int i = 0; i < elements; i++)
        {

            MPI_Recv(&stored[i], 1, MPI_FLOAT, 0, 8, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("worker %f received data\n",stored[i]);
        } //for recieving item for preprocessing

        //preprocess

        if (flag == 0)
        {
            ABC = preprocess_ABC(stored, elements, average);
        } //ABC

        else if (flag == 1)
        {
            stored = normalize(stored, size, average, sd);
            //printf("normalized\n");
            //this will be shorter
            SAX = preprocess_SAX(stored, elements, num_symbols, size, rank, word_length);
            //printf("preprocessed\n");
        } //SAX

        //receive index of the data item INT
        //receive elements FLOATS for the full data item
        //preprocess acording to flag

        //normalize for SAX

        //for SAX and ABC enough to preprocess
        //SAX use 0-based indexes for the symbols: 0 is a, 1 is b and so on
        //can use directly as indexes for breakpoints and distances

        /////////////////////////////////////////////////////////////////////
        //send preprocessed item to storage node with its index
        //printf("worker sending item %d to storage\n", item_index);

        if (flag == 0)
        {
            MPI_Send(&item_index, 1, MPI_INT, size - 5, 3, MPI_COMM_WORLD);
            for (int i = 0; i < elements; i++)
            {
                MPI_Send(&ABC[i], 1, MPI_INT, size - 5, 3, MPI_COMM_WORLD);
                //printf("processed data is %d\n", ABC[i]);
            } //end sending preprocessed item to storage
            printf("ABC sent for storage\n");
        }

        else if (flag == 1)
        {
                MPI_Send(&item_index, 1, MPI_INT, size - 5, 3, MPI_COMM_WORLD);
            for (int i = 0; i < length; i++)
            {
                MPI_Send(&SAX[i], 1, MPI_INT, size - 5, 3, MPI_COMM_WORLD);
            } //end sending preprocessed item to storage

            printf("SAX sent for storage\n");
        }

        /////////////////////////////////////////////////////////////////////
        //for abc and sax
        //request hash node
        //for each hash:
        //find that code
        //send code and item index to hashtable

        //for SSH create hash code and send to hashtable

        //SSH CREATING HASH CODE CODE
        if (flag == 2)
        {
            //change to proper parameters
            preprocess_SSH(rank, size, item_index, stored, elements, num_hash, step_hash, size_hash, num_symbols, word_length, hash_matrix, vector);

        } //SSH

        else
        {
            int data = 1;

            for (int j = 1; j <= num_hash; j++)
            {

                //printf("worker %d hash %d on item %d\n",rank, j, item_index);

                //create code
                //code = stored[hash[j]:hash[j]+size_hash-1];
                for (int i = 0; i < size_hash; i++)
                {

                    if (flag == 1)
                    {
                        code[i] = SAX[hash[j] + i];
                    }

                    if (flag == 0)
                    {
                        code[i] = ABC[hash[j] + i];
                    }
                } //for sending hash code

                //send hash and item index to hashtable
                int dest = j;

                MPI_Send(&item_index, 1, MPI_INT, dest, 9, MPI_COMM_WORLD);
                //printf("hash code %d ", j);
                for (int i = 0; i < size_hash; i++)
                {
                    //printf("%d", code[i]);
                    MPI_Send(&code[i], 1, MPI_INT, dest, 9, MPI_COMM_WORLD);
                    //printf("worker %d sending hashes on item %d, with index %d\n", rank, item_index, j);
                } //for sending hash code
                  //printf("\n");

            } //for each hash
            printf("worker %d done hashes on item %d\n", rank, item_index);
        } //hash and send for abc and sax
        //printf("worker %d looking for more work\n", rank);
    } //while

    //printf("worker %d done\n", rank);
    
    int data = 0;

    for (int i = 1; i <= num_hash; i++)
    {
        MPI_Send(&data, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        //printf("worker %d sent shut command to hashtable %d\n", rank, i);
    }

    free(stored);
    free(ABC);
    free(SAX);
    free(code);
    free(hash);
    free(hash_matrix);
    free(vector);

    //printf("worker %d done\n", rank);

} //worker

/******* MAIN *********/
//this represents one whole run of an experiment
//each parameter array starts with what experiment trial this is , n total of data items 13 and str filename 14
//start is the zero-based index of first element to include
//elements <= number of time steps for the data items - start
//ABC parameters flag 0
// 0 start elements  num_hash size_hash step_hash 2 0 average 0 sim
//SAX parameters flag 1
// 1 start elements num_hash size_hash step_hash num_symbols word_length average sd 0
//SSH parameters flag 2
// 2 start elements num_hash size_hash step_hash num_symbols (n for n-grams) overlap 0 0 0
//ADD num_items and filename parameters
//main initializes and assigns roles
int my_main(int argc, char **argv)
{

    //initialize
    MPI_Init(&argc, &argv);

    int rank, size;

    int trial;

    int flag, start, elements;
    int num_hash, size_hash, step_hash;
    int num_symbols, word_length;
    float average, sd, sim;

    int n;
    char filename[10000];

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

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

    int num_workers = size - num_hash - 6;

    //assign roles dependent on rank
    if (rank == 0)
    {
        manager_fn(elements, num_hash, size);
    }
    else if (rank <= num_hash)
    {
        hashtable_fn(size, size_hash, rank, flag, num_workers, n);
    }
    else if (rank == size - 1)
    {
        
       receiver_fn(start, elements, argv[14]);
    }
    else if (rank == size - 2)
    {
        writer_fn(trial, flag, size_hash, num_hash);
    }
    else if (rank == size - 3)
    {
        similarity_fn(flag, elements, num_symbols, word_length, sim, size, rank, num_hash);
    }
    else if (rank == size - 4)
    {
        if (flag == 0)
        {
            hash_fn_ABC(elements, num_hash, size_hash);
        }
        else if (flag == 1)
        {
            hash_fn_SAX(elements, num_hash, num_symbols, word_length, size_hash);
        }
        else
        {
            hash_fn_SSH(num_hash, size_hash, num_symbols);
        }
    }
    else if (rank == size - 5)
    {
        if (flag == 0)
        {
            storage_fn(n, elements);
        }
        else if (flag == 1)
        {
            storage_fn(n, elements / word_length);
        }
        else
        { 
       // printf("VALUE is %d\n", (int) pow(2, num_symbols));
            storage_fn(n, (int) pow(2, num_symbols));
        }
    }
    else
    {
        worker_fn(rank, flag, size_hash, step_hash, num_symbols, word_length, average, sd, size, elements, num_hash);
    }

    //clean up
    printf("rank %d done\n", rank);
    MPI_Finalize();
    return 0;
}
// own      own      own       send     send      send    send       send
//manager hashtables  workers  storage    hash      sim     writer     receiver
// 0      1-num_hash          size-5  size-4    size-3   size-2     size-1
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

/* to compile mpicc lsh.c -o lsh -lm */

/* to compile
mpicc lsh.c -o lsh
to run 
mpiexec -n  ./sigs num_elem num_sets num_hash size_hash (rest are workers)
with fgmpi (4 workers):
mpiexec -nfg 1 -n 22 ./sigs 5 8 4 4 
OR
mpiexec -nfg 22 -n 1 ./sigs 5 8 4 4
*/
