#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <stdarg.h>
#include <unistd.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define RANK_MAIN_PROCESS 0
#define CMD_START 1
#define CMD_STOP 2
#define CMD_CONFIG_HASH 3

#define METHOD_ABC 0
#define METHOD_SAX 1
#define METHOD_SSH 2



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
}

void logHash(struct Config *config) {
    for (int i=0; i<config->num_hash; i++) {
    	my_log(config, "index %d: %d", i, config->hash[i]);
    }
}

void stopAll(struct Config *config) {

	int cmd_stop = CMD_STOP;

	for (int i=1; i<config->cluster_size; i++) {
		my_log(config, "Stopping: %d", i);
		MPI_Send(&cmd_stop, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
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
	config->worker_count=1;

	switch (config->rank) {
		case RANK_MAIN_PROCESS:
			config->process_name = "Main";

		default:
			if (config->rank==1) {
				config->process_name = "Worker";
			}
	}
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

void worker_fn2(struct Config *config) {

	my_log(config, "worker_fn2");

	int running = 1;

	int cmd;

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
			    break;

		default:
			my_log(config, "Unknown command: %d", cmd);


		}

	}

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
//************************************//


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

    		my_log(config, "METHOD_ABC");

    		config->hash = random_indexes(config->num_hash, config->elements, config->size_hash);

    		logHash(config);

		    int cmd_config_hash=CMD_CONFIG_HASH;

		    for (int i=0; i<config->worker_count; i++) {
			    MPI_Send(&cmd_config_hash, 1, MPI_INT, config->rank_worker_start+i, 0, MPI_COMM_WORLD);
		    	MPI_Send(config->hash, config->num_hash, MPI_INT, config->rank_worker_start+i, 0, MPI_COMM_WORLD);
		    }

    		break;

 /*   	case 1:
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
*/
    }

    process_data_window(config);

	//usleep(11*1000*1000);

	stopAll(config);


}


int main(int argc, char **argv) {

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

    my_log(&config, "Start");

    switch (config.rank) {
    	case RANK_MAIN_PROCESS:
    		main_process_fn(&config);
    		break;

    	default:
    		worker_fn2(&config);
    		break;
    }

    freeConfig(&config);

    MPI_Finalize();

    my_log(&config, "Stop");

    return 0;
}

