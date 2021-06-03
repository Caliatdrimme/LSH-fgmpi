#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <stdarg.h>
#include <unistd.h>

#include <glib.h>

//#define MAX(x, y) (((x) > (y)) ? (x) : (y))
//#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define RANK_MAIN_PROCESS 0
//#define RANK_STORAGE 1
#define RANK_SIMILARITY 1
#define RANK_HASH_TABLE 2

typedef enum {
	TAG_WORKER_HASH,
	TAG_WORKER_AVAILABLE,
	TAG_WORKER_LINE_INDEX,
	TAG_WORKER_LINE_DATA,
	TAG_STORAGE_LINE_INDEX,
	TAG_STORAGE_DATA,
	TAG_HASHTABLE_LINE_INDEX, // 6
	TAG_HASHTABLE_LINE_DATA,
	TAG_SIMILARITY_PAIR_1,
	TAG_SIMILARITY_PAIR_2,
	TAG_SIMILARITY_WRITE,
	TAG_WORK_COMPLETE,

	TAG_STOP
} tag_t;

#define METHOD_ABC 0
#define METHOD_SAX 1
#define METHOD_SSH 2

#define LOG_LEVEL_DEBUG 1
#define LOG_LEVEL_TRACE 2

#define LOG_LEVEL LOG_LEVEL_DEBUG

struct file_data_t {
	int len;
	int column_count;
	int line_count;
	float *data;
};

struct Config {

	int rank;
	int cluster_size;

    int trial;

    int flag, start, elements;
    int num_hash, size_hash, step_hash;
    int num_symbols, word_length;
    float average, sd, sim;

    int n;

    char* filename;

    // used only in the main process
    struct file_data_t file_data;

    int *hash;
    float *hash_SSH;
    float *distances;
    float *breakpoints;
    float *perm;

    int rank_worker_start;
    int worker_count;

    char* process_name;
};

void freeConfig(struct Config *config) {
	if (config->file_data.data!=NULL) {
		free(config->file_data.data);
	}
}

typedef struct Hashtable
{
    int n;
    int *codes;
    int *counts;
    int *items;
    int index;
    int c;
} hashtable;

/*
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

    //int c = table.counts[ind];
    //printf("index %d and count %d\n", ind, c);

        int s = 0;

        for (int i = 0; i < table.n -1; i++)
        {
            // printf("here! table.index=%d\n", table.index);
            s = s + table.counts[i];
        } //for calculating size of items array

        table.items = resize_table(table.items, s);

        table.c = s;
        // *(table.items + s) = item;
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

*/

void my_log(struct Config *config, char* s, ...) {

	char buff[100];
	time_t now = time (0);
	strftime (buff, 100, "%H:%M:%S", localtime (&now));

	printf("%d | %s | %s | ", config->rank, config->process_name, buff);

	// magic
	va_list argptr;
	va_start(argptr, s);
	vfprintf(stdout, s, argptr);
	va_end(argptr);

	printf ("\n");
	fflush(stdout);
}

void log_hash(struct Config *config) {
	char buf[1000];
	char *s=buf;
	for (int i=0; i<config->num_hash; i++) {
    	s = (char*) (buf+sprintf(s, "index %d: %d ", i, config->hash[i]));
    }
	my_log(config, "Hash: %s", buf);

}

void stopAll(struct Config *config) {

	//int cmd_stop = CMD_STOP;

	for (int i=1; i<config->cluster_size; i++) {
		my_log(config, "Stopping: %d", i);
		MPI_Send(0, 0, MPI_INT, i, TAG_STOP, MPI_COMM_WORLD);
	}
}

void init_config(struct Config *config, char **argv) {
	config->trial = atoi(argv[1]);

	config->flag = atoi(argv[2]);

	config->start = atoi(argv[3]);

	// time series length
	config->elements = atoi(argv[4]);

	config->num_hash = atoi(argv[5]);
	config->size_hash = atoi(argv[6]);
	config->step_hash = atoi(argv[7]);

	config->num_symbols = atoi(argv[8]);
	config->word_length = atoi(argv[9]);

	config->average = atof(argv[10]);
	config->sd = atof(argv[11]);
	config->sim = atof(argv[12]);

	config->n = atoi(argv[13]);

	config->filename = argv[14];

	config->rank_worker_start=1;
	config->worker_count=0;

	config->process_name = malloc(100 * sizeof(char));

	config->process_name[0] = 0;

}

void print_config(struct Config *config) {
	printf("1.  trial: %d\n", config->trial);
	printf("2.  flag: %d\n", config->flag);
	printf("3.  start: %d\n", config->start);
	printf("4.  elements: %d\n", config->elements);
	printf("5.  num_hash: %d\n", config->num_hash);
	printf("6.  size_hash: %d\n", config->size_hash);
	printf("7.  step_hash: %d\n", config->step_hash);
	printf("8.  num_symbols: %d\n", config->num_symbols);
	printf("9.  word_length: %d\n", config->word_length);
	printf("10. average: %f\n", config->average);
	printf("11. sd: %f\n", config->sd);
	printf("12. sim: %f\n", config->sim);
	printf("13. n: %d\n", config->n);
	printf("14. filename: %s\n", config->filename);
}


void receive_worker_data(struct Config *config, float *data) {

}

void send_worker_data(struct Config *config, float *data) {
	// MPI_Send(data, config->elements, MPI_Float, dest, tag, MPI_COMM_WORLD);
	receive_worker_data(config, data);
}

void process_data_window(struct Config *config) {
	for (int line=0; line<config->file_data.line_count; line++) {
		send_worker_data(config, &(config->file_data.data[line*config->elements + config->start]));
	}
}

int *abc_preprocess(float *item, int elements, float average)
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


struct ABC_data_t {
	int line_index;
	int *data;
};

void process_requests(struct Config *config);


void worker_fn2(struct Config *config) {

	my_log(config, "worker_fn2");

	process_requests(config);

/*
	int running = 1;

	int cmd;

    int cmd_worker_available = CMD_WORKER_AVAILABLE;

	while (running) {
		my_log(config, "Receiving...");
		MPI_Recv(&cmd, 1, MPI_INT, RANK_MAIN_PROCESS, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		switch(cmd) {

		case CMD_STOP:
				my_log(config, "Cmd stop");
				running = 0;
				break;

		case CMD_CONFIG_HASH:
			    my_log(config, "Cmd config hash");
				config->hash = malloc(config->num_hash*sizeof(int));
			    MPI_Recv(config->hash, config->num_hash, MPI_INT, RANK_MAIN_PROCESS, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			    logHash(config);

			    MPI_Send(&cmd_worker_available, 1, MPI_INT, RANK_MAIN_PROCESS, 0, MPI_COMM_WORLD);
			    break;

		case CMD_WORKER_DATA:

				my_log(config, "Cmd worker data");
				float *data = (float *)malloc(config->elements * sizeof(float));

		        MPI_Recv(data, config->elements, MPI_FLOAT, RANK_MAIN_PROCESS, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		        if (LOG_LEVEL == LOG_LEVEL_DEBUG) {
		        	my_log(config, "Data");
		        	for (int i=0; i<config->elements; i++) {
		        		printf("%f ", data[i]);
		        	}
		        	printf("\n");
		        }

		        //struct ABC_data_t abc_data;
		        //abc_data.line_index

		        preprocess_ABC(data, config->elements, config->average);

		        int cmd_abc_store = CMD_ABC_STORE;

		        MPI_Send(&cmd_abc_store, 1, MPI_INT, RANK_STORAGE, 0, MPI_COMM_WORLD);


		    	MPI_Send(&cmd_worker_available, 1, MPI_INT, RANK_MAIN_PROCESS, 0, MPI_COMM_WORLD);
				break;

		default:
			my_log(config, "Unknown command: %d", cmd);


		}

	}
*/

}

// returns 1, if success; 0 if failure

int read_file(struct Config *config) {

	// the file is a matrix WxH of floats separated by space characters. We assume the file is always valid, i.e.
	// all the characters are valid floats, spaces, or new line

    FILE *fp;
    fp = fopen(config->filename, "r");

    if (fp==NULL) {
    	my_log(config, "Cannot open file %s", config->filename);
    	return 0;
    }

	config->file_data.data = (float*)malloc(100000*sizeof(float));
	int len=0;

    char str[100000];

    int line=0;

    int current_column_count;
    int target_column_count = 0;


    while (fgets(str, sizeof(str), fp))
    {

        char *ptr = str, *eptr;

        // if strtof cannot parse a float, eptr will be equal to ptr. We assume it will happen only at the end of the line
        float f = strtof(ptr, &eptr);

        if (ptr == eptr) {
        	// we could not parse even a single float from the line, which means the line is empty. Assume it's the last line
        	break;
        }

        line ++;

        current_column_count = 0;

        while (ptr != eptr) {

           current_column_count++;

           config->file_data.data[len] = f;

           len++;

           ptr = eptr;
           f = strtof(ptr, &eptr);
        }

        if (target_column_count==0) {
        	target_column_count = current_column_count;
        }
        else {
        	if (target_column_count!=current_column_count) {
        		my_log(config, "Invalid column count in line %d, expected %d", line, target_column_count);
        		return 0;
        	}
        }
    }

    my_log(config, "Read file: len: %d line count: %d column count: %d", len, line, target_column_count);

    config->file_data.line_count = line;
    config->file_data.column_count = target_column_count;
    config->file_data.len = len;

    return 1;
} // read_file

/******* HASH *********/

int cmpfunc(const void *a, const void *b)
{
    return (*(int *)a - *(int *)b);
}


// select n random substrings of length size_hash
// return the indexes of the substring starts
// in other words, returns array of size n
// holds random indexes between 0 and elements-size_hash
int *random_indexes(int n, int elements, int size_hash)
{

    srand(time(NULL));

    int *indexes;
    indexes = (int *)malloc(sizeof(int) * n);

    int index_range = elements - size_hash+1;

    int selection[index_range];
    for (int i=0; i<index_range-1; i++) {
    	selection[i] = 1;
    }

    for (int i=0; i<n; i++) {
    	int index = rand() % index_range;
    	while (selection[index]<0) {
    		index++;
    		if (index>=index_range) {
    			index = 0;
    		}
    	}
    	indexes[i] = index;
    	selection[index] = -1;
    }

    qsort(indexes, n, sizeof(indexes[0]), cmpfunc);

    return indexes;

} //random_indexes

/*
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

*/

int calc_worker_data_packed_size(struct Config *config) {
	int buf_size_1;
	MPI_Pack_size(1, MPI_INT, MPI_COMM_WORLD, &buf_size_1);

	int buf_size_2;
	MPI_Pack_size(config->elements, MPI_FLOAT, MPI_COMM_WORLD, &buf_size_2);

	int buf_size = buf_size_1+buf_size_2;

	return buf_size;
}

void abc_main_process(struct Config *config) {
	my_log(config, "METHOD_ABC");

	config->hash = random_indexes(config->num_hash, config->elements, config->size_hash);

	log_hash(config);

    for (int i=0; i<config->worker_count; i++) {
    	MPI_Send(config->hash, config->num_hash, MPI_INT, config->rank_worker_start+i, TAG_WORKER_HASH, MPI_COMM_WORLD);
    }

    int line_index = 0;

    while (line_index < config->file_data.line_count) {

    	MPI_Status mpi_status;

    	my_log(config, "Waiting for a worker");

    	MPI_Recv(NULL, 0, MPI_INT, MPI_ANY_SOURCE, TAG_WORKER_AVAILABLE, MPI_COMM_WORLD, &mpi_status);

    	my_log(config, "Worker available: %d for line: %d", mpi_status.MPI_SOURCE, line_index);

	    float *data = &config->file_data.data[line_index*config->file_data.column_count + config->start];

	    MPI_Send(&line_index, 1, MPI_INT, mpi_status.MPI_SOURCE, TAG_WORKER_LINE_INDEX, MPI_COMM_WORLD);
	    MPI_Send(data, config->elements, MPI_FLOAT, mpi_status.MPI_SOURCE, TAG_WORKER_LINE_DATA, MPI_COMM_WORLD);

	    line_index++;

    }

    // tell the workers we are done
    for (int i=0; i<config->worker_count; i++) {
    	MPI_Send(NULL, 0, MPI_INT, config->rank_worker_start+i, TAG_WORK_COMPLETE, MPI_COMM_WORLD);
    }

	int running=1;

	// wait for work complete from similarity

	while (running) {

		int flag;
		MPI_Status status;

		MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

		if (flag) {
			if (LOG_LEVEL==LOG_LEVEL_TRACE) {
				my_log(config, "Request iprobed; source: %d tag: %d", status.MPI_SOURCE, status.MPI_TAG);
			}

			switch((tag_t)status.MPI_TAG) {

				case TAG_WORKER_AVAILABLE: {
					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
					break;
				}

				case TAG_WORK_COMPLETE: {

					my_log(config, "Work complete from similarity");

					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
					running = 0;

			        break;
				}

				default:
					my_log(config, "Unexpected message: %d ", status.MPI_TAG);

			}

		}
		else {
			usleep(1000L);
		}

	}

}

void main_process_fn(struct Config *config) {

	my_log(config, "Cluster size: %d", config->cluster_size);
	printf("config: \n");
	print_config(config);

	int success = read_file(config);

    if (!success) {
    	stopAll(config);
        return;
    }

    switch (config->flag) {
    	case METHOD_ABC:
    		abc_main_process(config);
    		break;
    }

/*
    switch (config->flag) {
    	case METHOD_ABC:
    		main_process_method_abc(config);
    		break;

    	case 1:
    	    config->hash = random_indexes(config->num_hash, config->elements / config->word_length, config->size_hash);
    	    // MPI_Send(&hash[i], 1, MPI_INT, dest, 4, MPI_COMM_WORLD);
    	    config->breakpoints = create_breakpoints(config->num_symbols);
    	    //MPI_Send(&breakpoints[i], 1, MPI_FLOAT, dest, 6, MPI_COMM_WORLD); length num_symbols
    	    config->distances = create_distances(config->num_symbols, config->breakpoints);
    	    //MPI_Send(&*(distances + s1 * config->num_symbols + s2), 1, MPI_FLOAT, status.MPI_SOURCE, 7, MPI_COMM_WORLD);
    		break;

    	case 2:
    	    config->hash_SSH = random_vector(config->size_hash);
    	    //MPI_Send(&hash[i], 1, MPI_FLOAT, dest, 4, MPI_COMM_WORLD);
    	    //create perm num_hash X l floats random from 0 to 2**num_symbols

    	    int k = pow(2, config->num_symbols);

    	    int l = k / 2;

    	    float *perm = create_random_matrix(config->num_hash, l, k);
    	    //MPI_Send(&perm[i*l + j], 1, MPI_FLOAT, dest, 6, MPI_COMM_WORLD); i num_hash j l
    		break;
    }
*/

	//MPI_Send(NULL, 0, MPI_INT, RANK_SIMILARITY, TAG_SIMILARITY_WRITE, MPI_COMM_WORLD);

	//usleep(5*1000*1000);

	//stopAll(config);


}

guint g_array_hash(gconstpointer  v) {

	int length = *(int *)v;

	int hash = 17;
	int *position = (int *)v;
	position ++;
	for (int i = 0; i < length; i++) {
		int value = *position++;
	    hash = hash * 31 + value;
	}
	return hash;
}

gboolean g_array_equal(gconstpointer a, gconstpointer b) {

	int *array_a = (int *)a;
	int *array_b = (int *)b;

	int a_length = *array_a;
	int b_length = *array_b;

	if (a_length!=b_length) {
		return FALSE;
	}

	array_a++;
	array_b++;

	for (int i=0; i<a_length; i++) {
		if (*array_a++ != *array_b++) {
			return FALSE;
		}
	}

	return TRUE;
}

void abc_worker_hashtable(struct Config *config) {

	GHashTable* hash = g_hash_table_new(g_array_hash, g_array_equal);

	int running=1;

	//char *data = malloc(sizeof(int) * config->size_hash * config->n );

	int work_complete_count=0;

	while (running) {

		int flag;
		MPI_Status status;

		MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

		if (flag) {
			if (LOG_LEVEL==LOG_LEVEL_TRACE) {
				my_log(config, "Request iprobed; source: %d tag: %d", status.MPI_SOURCE, status.MPI_TAG);
			}

			switch((tag_t)status.MPI_TAG) {
				case TAG_STOP:
					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
					running = 0;
					my_log(config, "Stopping");
					break;

				case TAG_HASHTABLE_LINE_INDEX: {

					int line_index;
					MPI_Recv(&line_index, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

					my_log(config, "Received line index: %d from %d", line_index, status.MPI_SOURCE);

					int *data = malloc(sizeof(int) * (config->size_hash + 1));

					*data = config->size_hash;

					MPI_Recv(&data[1], config->size_hash, MPI_INT, status.MPI_SOURCE, TAG_HASHTABLE_LINE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					my_log(config, "Hash code data received; line: %d ", line_index);

			        if (LOG_LEVEL == LOG_LEVEL_DEBUG) {
			        	my_log(config, "Data");
			        	for (int i=0; i<config->size_hash+1; i++) {
			        		printf("%d ", data[i]);
			        	}
			        	printf("\n");
			        }

			        GSList *list = g_hash_table_lookup(hash, data);

			        if (list!=NULL) {
			        	GSList *element = list;
			        	while (element != NULL) {
			        		long value = (long)element->data;

			        		int value_int = (int) value;

			        		MPI_Send(&line_index, 1, MPI_INT, RANK_SIMILARITY, TAG_SIMILARITY_PAIR_1, MPI_COMM_WORLD);
			        		MPI_Send(&value_int, 1, MPI_INT, RANK_SIMILARITY, TAG_SIMILARITY_PAIR_2, MPI_COMM_WORLD);

			        		element = element->next;
			        	}
			        }

			        long li = line_index;
			        list = g_slist_prepend(list, (gpointer)li);

		        	g_hash_table_insert(hash, data, list);

					break;
				}

				case TAG_WORK_COMPLETE: {

					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);

					my_log(config, "Work complete from: %d", status.MPI_SOURCE);

					work_complete_count++;
					if (work_complete_count>=config->worker_count) {
						my_log(config, "All workers are done; sending work complete to similarity");
				        MPI_Send(NULL, 0, MPI_INT, RANK_SIMILARITY, TAG_WORK_COMPLETE, MPI_COMM_WORLD);
				        running = 0;
					}


			        break;
				}

				default:

					my_log(config, "Unexpected message: %d ", status.MPI_TAG);

			}

		}
		else {
			usleep(1000L);
		}


	} // running


} // abc_worker_hashtable

//calculates the similarity by ABC
float abc_sim(int *item1, int *item2, float sim, int elements)
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


void abc_worker_similarity(struct Config *config) {

	int running=1;

	int *data = malloc(sizeof(int) * config->elements * config->n );

	int line_count = 0;

	int processed_pairs[config->n][config->n];

	for (int i=0; i<config->n; i++) {
		for (int j=0; j<config->n; j++) {
			processed_pairs[i][j] = 0;
		}
	}

	float similarity_matrix[config->n][config->n];

	int work_complete_count=0;

	while (running) {

		//my_log(config, "Probing...");

		int flag;
		MPI_Status status;

		MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

		if (flag) {
			if (LOG_LEVEL==LOG_LEVEL_TRACE) {
				my_log(config, "Request iprobed; source: %d tag: %d", status.MPI_SOURCE, status.MPI_TAG);
			}

			switch((tag_t)status.MPI_TAG) {
				case TAG_STOP:
					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
					running = 0;
					my_log(config, "Stopping");
					break;

				case TAG_STORAGE_LINE_INDEX: {

					int line_index;
					MPI_Recv(&line_index, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

					MPI_Recv(&data[line_index*config->elements], config->elements, MPI_INT, status.MPI_SOURCE, TAG_STORAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			        line_count++;

					my_log(config, "Storage data received; line: %d line_count: %d", line_index, line_count);

			        if (LOG_LEVEL == LOG_LEVEL_DEBUG) {
			        	my_log(config, "Data");
			        	for (int i=0; i<config->elements; i++) {
			        		printf("%d ", data[line_index*config->elements + i]);
			        	}
			        	printf("\n");
			        }

			        my_log(config, "end of TAG_STORAGE_LINE_INDEX");

					break;
				}

				case TAG_SIMILARITY_PAIR_1: {

					//my_log(config, "start of TAG_SIMILARITY_PAIR_1");

					int line1;
					int line2;

					MPI_Recv(&line1, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(&line2, 1, MPI_INT, status.MPI_SOURCE, TAG_SIMILARITY_PAIR_2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

					my_log(config, "Pair received: %d & %d from %d", line1, line2, status.MPI_SOURCE);

					if (processed_pairs[line1][line2] == 1) {
						my_log(config, "The pair already processed");
					}
					else {
						processed_pairs[line1][line2] = 1;
						processed_pairs[line2][line1] = 1;

						float similarity = abc_sim(&data[line1*config->elements], &data[line2*config->elements], config->sim, config->elements);

						similarity_matrix[line1][line2] = similarity;
						similarity_matrix[line2][line1] = similarity;
					}

					break;
				}

				case TAG_WORK_COMPLETE: {

					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);

					my_log(config, "Work complete from: %d", status.MPI_SOURCE);

					work_complete_count++;
					if (work_complete_count>=config->num_hash) {
						my_log(config, "All hash tables are done; writing and sending work complete to main process");

						my_log(config, "Writing...");

						char name[1000];

						sprintf(name, "similarity_abc_%05d.csv", config->trial);

						FILE *fp;

					    fp = fopen(name, "w");

					    fprintf(fp, "HASH_SIZE, LINE_1, LINE_2, SIMILARITY\n");

						for (int i=0; i<config->n; i++) {
							for (int j=0; j<i; j++) {
								fprintf(fp, "%d, %d, %d, %f\n", config->size_hash, i, j, similarity_matrix[i][j]);
							}
						}

						fclose(fp);

						MPI_Send(NULL, 0, MPI_INT, RANK_MAIN_PROCESS, TAG_WORK_COMPLETE, MPI_COMM_WORLD);
				        running = 0;
					}


			        break;
				}


				case TAG_SIMILARITY_WRITE: {

					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

					break;
				}

				default:

					my_log(config, "Unexpected message: %d ", status.MPI_TAG);

			}

		}
		else {
			// 1 millisecond
			usleep(1000L);
			//usleep(1000L * 1000L * 10);
		}


	} // running

	free(data);

}

void abc_worker(struct Config *config) {

	int running=1;

	while (running) {

		int flag;
		MPI_Status status;

		MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

		if (flag) {
			if (LOG_LEVEL==LOG_LEVEL_TRACE) {
				my_log(config, "Request iprobed; source: %d tag: %d", status.MPI_SOURCE, status.MPI_TAG);
			}

			switch((tag_t)status.MPI_TAG) {
				case TAG_STOP:
					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
					running = 0;
					my_log(config, "Stopping");
					break;

					// main process sent us the hash indexes
				case TAG_WORKER_HASH:
					config->hash = malloc(config->num_hash*sizeof(int));
					MPI_Recv(config->hash, config->num_hash, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_HASH, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					log_hash(config);
					MPI_Send(NULL, 0, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_AVAILABLE, MPI_COMM_WORLD);
					break;

					// main process sent us a data line (line number [index], followed by the data)
				case TAG_WORKER_LINE_INDEX: {

					int line_index;
					float *data = (float *) malloc(config->elements * sizeof(float));

					MPI_Recv(&line_index, 1, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_LINE_INDEX, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(data, config->elements, MPI_FLOAT, RANK_MAIN_PROCESS, TAG_WORKER_LINE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			        if (LOG_LEVEL == LOG_LEVEL_DEBUG) {
			        	my_log(config, "Data");
			        	for (int i=0; i<config->elements; i++) {
			        		printf("%f ", data[i]);
			        	}
			        	printf("\n");
			        }

			        int *preprocessed = abc_preprocess(data, config->elements, config->average);

			        MPI_Send(&line_index, 1, MPI_INT, RANK_SIMILARITY, TAG_STORAGE_LINE_INDEX, MPI_COMM_WORLD);

			        MPI_Ssend(preprocessed, config->elements, MPI_INT, RANK_SIMILARITY, TAG_STORAGE_DATA, MPI_COMM_WORLD);

			        // calculate the hash codes and send them to the hash table

			        for (int hash_table_index=0; hash_table_index<config->num_hash; hash_table_index++) {

			        	int hash_code[config->size_hash];

			        	// config->hash contains starts of substrings. Here we build the substrings of the pre-porcessed data

			        	int substring_start = config->hash[hash_table_index];

			        	for (int i=0; i<config->size_hash; i++) {
			        		hash_code[i] = preprocessed[substring_start + i];
			        	}

			        	MPI_Send(&line_index, 1, MPI_INT, RANK_HASH_TABLE+hash_table_index, TAG_HASHTABLE_LINE_INDEX, MPI_COMM_WORLD);
			        	MPI_Send(&hash_code, config->size_hash, MPI_INT, RANK_HASH_TABLE+hash_table_index, TAG_HASHTABLE_LINE_DATA, MPI_COMM_WORLD);
			        }

					MPI_Send(NULL, 0, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_AVAILABLE, MPI_COMM_WORLD);

					free(data);
					free(preprocessed);

			        break;
				}

				case TAG_WORK_COMPLETE: {

					my_log(config, "Work complete");

					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
					running = 0;

			        for (int hash_table_index=0; hash_table_index<config->num_hash; hash_table_index++) {
			        	MPI_Send(NULL, 0, MPI_INT, RANK_HASH_TABLE+hash_table_index, TAG_WORK_COMPLETE, MPI_COMM_WORLD);
			        }

			        break;
				}

				default:
					my_log(config, "Unexpected message: %d ", status.MPI_TAG);

			}

		}
		else {
			usleep(1000L);
		}


	}

}


void process_requests(struct Config *config) {

	int running=1;

	while (running) {

		int flag;
		MPI_Status status;

		MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

		if (flag) {
			if (LOG_LEVEL==LOG_LEVEL_TRACE) {
				my_log(config, "Request iprobed; source: %d tag: %d", status.MPI_SOURCE, status.MPI_TAG);
			}

			//int line_index;
			//float *data = (float *) malloc(config->elements * sizeof(float));

			//char *storage_data_abc = (char *) malloc(config->elements * sizeof(char));

			switch((tag_t)status.MPI_TAG) {
				case TAG_STOP:
					MPI_Recv(NULL, 0, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
					running = 0;
					my_log(config, "Stopping");
					break;

/*
				case TAG_WORKER_HASH:
					config->hash = malloc(config->num_hash*sizeof(int));
					MPI_Recv(config->hash, config->num_hash, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_HASH, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					log_hash(config);
					MPI_Send(NULL, 0, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_AVAILABLE, MPI_COMM_WORLD);
					break;

				case TAG_WORKER_LINE_INDEX:
					MPI_Recv(&line_index, 1, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_LINE_INDEX, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(data, config->elements, MPI_FLOAT, RANK_MAIN_PROCESS, TAG_WORKER_LINE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			        if (LOG_LEVEL == LOG_LEVEL_DEBUG) {
			        	my_log(config, "Data");
			        	for (int i=0; i<config->elements; i++) {
			        		printf("%f ", data[i]);
			        	}
			        	printf("\n");
			        }

			        char *preprocessed = preprocess_ABC(data, config->elements, config->average);

			        MPI_Ssend(preprocessed, config->elements, MPI_CHAR, RANK_STORAGE, TAG_STORAGE_DATA, MPI_COMM_WORLD);

					MPI_Send(NULL, 0, MPI_INT, RANK_MAIN_PROCESS, TAG_WORKER_AVAILABLE, MPI_COMM_WORLD);

			        break;

				case TAG_STORAGE_DATA:

					MPI_Recv(storage_data_abc, config->elements, MPI_CHAR, status.MPI_SOURCE, TAG_STORAGE_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					my_log(config, "Storage data received");

					break;
*/

				default:
					my_log(config, "Unexpected message: %d ", status.MPI_TAG);

			}

		}
		else {
			usleep(1000L);
		}


	}

}


int main(int argc, char **argv) {

	// --oversubscribe -v -output-filename ~/lsh_log

    //initialize
    MPI_Init(&argc, &argv);

    struct Config config;

    config.file_data.data = NULL;
    config.hash = NULL;

    MPI_Comm_rank(MPI_COMM_WORLD, &config.rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.cluster_size);

	if (argc<15) {
		if (config.rank==0) {
			printf("Required parameters(14): trial flag start elements num_hash size_hash step_hash num_symbols word_length average sd sim n filename\n");
			printf("Example: 1 0 0 6 3 3 1 2 1 2.0 0.1 0.1 3 data.txt\n");
		}

		MPI_Finalize();
		return 1;
	}

	// fill the config struct with the values from the command line
	init_config(&config, argv);

	int min_cluster_size = 3+config.num_hash+1;

	if (config.cluster_size<min_cluster_size) {
		if (config.rank==0) {
			printf("This program requires at least %d processes\n", min_cluster_size);
		}
		return 1;
	}

	config.rank_worker_start = RANK_HASH_TABLE + config.num_hash;
	config.worker_count = config.cluster_size - config.rank_worker_start;

    switch (config.rank) {
    	case RANK_MAIN_PROCESS:
    		config.process_name = "Main";
    	    my_log(&config, "Start");
    		my_log(&config, "Worker start rank: %d worker count: %d", config.rank_worker_start, config.worker_count );
    		main_process_fn(&config);
    		break;

/*
    	case RANK_STORAGE:
    		config.process_name = "Storage";
    		my_log(&config, "Start");
    		abc_worker_storage(&config);
    		break;
*/

    	case RANK_SIMILARITY:
    		config.process_name = "Similarity";
    		my_log(&config, "Start");
    		abc_worker_similarity(&config);
    		break;

    	default:

    		if (config.rank>=RANK_HASH_TABLE && config.rank<RANK_HASH_TABLE+config.num_hash) {
    			int hash_table_index = config.rank - RANK_HASH_TABLE;
    			sprintf(config.process_name, "Hash table %d", hash_table_index);
    			my_log(&config, "Start");
    			// do hash table
    			abc_worker_hashtable(&config);
    		}
    		else {

    			int worker_index = config.rank - config.rank_worker_start;
    			sprintf(config.process_name, "Worker %d", worker_index);

    			my_log(&config, "Start");

    			abc_worker(&config);
    		}

    		break;
    }


    MPI_Finalize();

    my_log(&config, "Stop");

    return 0;
}

