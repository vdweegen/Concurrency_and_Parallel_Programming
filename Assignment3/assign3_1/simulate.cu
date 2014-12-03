/*
 * simulate.c
 *
 * Implement your (parallel) simulation here!
 */
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <iostream>

#include "timer.h"
#include "file.h"
#include "simulate.h"


using namespace std;

#define MAX_BLOCK_SIZE 512

/* Copied from vector-add.cu */
static void checkCudaCall(cudaError_t result) {
    if (result != cudaSuccess) {
        cerr << "cuda error: " << cudaGetErrorString(result) << endl;
        exit(1);
    }
}

 /* This we run on a GPU */
__global__ void calculate_next(double *dev_old, double *dev_cur,
        double *dev_new, int i_max, int timestep) {

    /* Define timesteps for */
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x + 1;
    unsigned int t_id = threadIdx.x;
    
    if (i < i_max) {
     __shared__ double current[MAX_BLOCK_SIZE];
     
     current[t_id] = dev_cur[i];
     __syncthreads();
     
     /* The formula from Assign 2_1 */
     if (t_id == 0){
      dev_new[i] = 2 * current[t_id] - dev_old[i] + 0.2 *
      (dev_cur[i - 1] - (2 * current[t_id] - current[t_id + 1]));
     }else if (t_id == blockDim.x - 1) {
      dev_new[i] = 2 * current[t_id] - dev_old[i] + 0.2 *
      (current[t_id - 1] - (2 * current[t_id] - dev_cur[i + 1]));
     }else {
      dev_new[i] = 2 * current[t_id] - dev_old[i] + 0.2 *
      (current[t_id - 1] - (2 * current[t_id] - current[t_id + 1]));
     }
    }
}

/* SIMULATOR FROM ASSIGNMENT 2_1 FRAMEWORK */
double *simulate(const int i_max, const int t_max, const int num_threads,
        double *old_array, double *current_array, double *next_array){
   double *dev_old, *dev_cur, *dev_new;

    /* Allocate data (as shown in vector-add.cu, ln 20-25) */
    checkCudaCall(cudaMalloc(&dev_old, i_max * sizeof(double)));
    checkCudaCall(cudaMalloc(&dev_cur, i_max * sizeof(double)));
    checkCudaCall(cudaMalloc(&dev_new, i_max * sizeof(double)));

    /* Start CUDA events (vector-add.cu, ln 60-63) */
    cudaEvent_t start, stop;
    cudaEventCreate(&start);
    cudaEventCreate(&stop);

    /* Copy Vector Data (vector-add.cu, ln 65) */
    checkCudaCall(cudaMemcpy(dev_old, old_array, i_max * sizeof(double), cudaMemcpyHostToDevice));
    checkCudaCall(cudaMemcpy(dev_cur, current_array, i_max * sizeof(double), cudaMemcpyHostToDevice));
    checkCudaCall(cudaMemcpy(dev_new, next_array, i_max * sizeof(double), cudaMemcpyHostToDevice));

    /* Start Timer (vector-add.cu) */
    cudaEventRecord(start, 0);
    
    /* Repeat for Number of Threads */
    for (int t = 1; t < t_max; t++) {
        /* See function definition above,
         * since we did this in a loop in the previous assignments
         */
        calculate_next<<<ceil((double)i_max/num_threads), num_threads>>>(
                dev_old, dev_cur, dev_new, i_max - 1, t);
        
        /* Do the Switcharoo */
        double *temp = dev_old;
        dev_old = dev_cur;
        dev_cur = dev_new;
        dev_new = temp;
    }

   /* Stop Timer (vector-add.cu) */
   cudaEventRecord(stop, 0);
    
   /* Check for Errors (vector-add.cu) */
    checkCudaCall(cudaGetLastError());

   /* Copy back results (vector-add.cu) */
   checkCudaCall(cudaMemcpy(current_array, dev_cur, i_max * sizeof(double), cudaMemcpyDeviceToHost));
    
   /* Free (vector-add.cu) */
   checkCudaCall(cudaFree(dev_old));
   checkCudaCall(cudaFree(dev_cur));
   checkCudaCall(cudaFree(dev_new));

   /* You should return a pointer to the array with the final results. */
   float elapsedTime;
   cudaEventElapsedTime(&elapsedTime, start, stop);

   printf("time spent in kernel: %f miliseconds\n", elapsedTime);
   return current_array;
}

typedef double (*func_t)(double x);

/* Copied from Assignment 2_1 (with minor modifications) */
double gauss(double x)
{
    return exp((-1 * x * x) / 2);
}

/* Copied from Assignment 2_1 (with minor modifications) */
void fill(double *array, int offset, int range, double sample_start,
        double sample_end, func_t f)
{
    int i;
    float dx;

    dx = (sample_end - sample_start) / range;
    for (i = 0; i < range; i++) {
        array[i + offset] = f(sample_start + i * dx);
    }
}

/* Copied from Assignment 2_1 (with minor modifications) */
int main(int argc, char *argv[])
{
    double *old, *current, *next, *ret;
    int t_max, i_max, num_threads;
    timer vectorAddTimer("vector add timer");


    /* Parse commandline args: i_max t_max num_threads */
    if (argc < 4) {
        printf("Usage: %s i_max t_max num_threads [initial_data]\n", argv[0]);
        printf(" - i_max: number of discrete amplitude points, should be >2\n");
        printf(" - t_max: number of discrete timesteps, should be >=1\n");
        printf(" - num_threads: number of threads to use for simulation, "
                "should be >=1\n");
        printf(" - initial_data: select what data should be used for the first "
                "two generation.\n");
        printf("   Available options are:\n");
        printf("    * sin: one period of the sinus function at the start.\n");
        printf("    * sinfull: entire data is filled with the sinus.\n");
        printf("    * gauss: a single gauss-function at the start.\n");
        printf("    * file <2 filenames>: allows you to specify a file with on "
                "each line a float for both generations.\n");

        return EXIT_FAILURE;
    }

    i_max = atoi(argv[1]);
    t_max = atoi(argv[2]);
    num_threads = atoi(argv[3]);

    if (i_max < 3) {
        printf("argument error: i_max should be >2.\n");
        return EXIT_FAILURE;
    }
    if (t_max < 1) {
        printf("argument error: t_max should be >=1.\n");
        return EXIT_FAILURE;
    }
    if (num_threads < 1) {
        printf("argument error: num_threads should be >=1.\n");
        return EXIT_FAILURE;
    }

    /* Allocate and initialize buffers. */
    old = (double *) malloc(i_max * sizeof(double));
    current = (double *) malloc(i_max * sizeof(double));
    next = (double *) malloc(i_max * sizeof(double));

    if (old == NULL || current == NULL || next == NULL) {
        fprintf(stderr, "Could not allocate enough memory, aborting.\n");
        return EXIT_FAILURE;
    }

    memset(old, 0, i_max * sizeof(double));
    memset(current, 0, i_max * sizeof(double));
    memset(next, 0, i_max * sizeof(double));

    /* How should we will our first two generations? */
    if (argc > 4) {
        if (strcmp(argv[4], "sin") == 0) {
            fill(old, 1, i_max/4, 0, 2*3.14, sin);
            fill(current, 2, i_max/4, 0, 2*3.14, sin);
        } else if (strcmp(argv[4], "sinfull") == 0) {
            fill(old, 1, i_max-2, 0, 10*3.14, sin);
            fill(current, 2, i_max-3, 0, 10*3.14, sin);
        } else if (strcmp(argv[4], "gauss") == 0) {
            fill(old, 1, i_max/4, -3, 3, gauss);
            fill(current, 2, i_max/4, -3, 3, gauss);
        } else if (strcmp(argv[4], "file") == 0) {
            if (argc < 7) {
                printf("No files specified!\n");
                return EXIT_FAILURE;
            }
            file_read_double_array(argv[5], old, i_max);
            file_read_double_array(argv[6], current, i_max);
        } else {
            printf("Unknown initial mode: %s.\n", argv[4]);
            return EXIT_FAILURE;
        }
    } else {
        /* Default to sinus. */
        fill(old, 1, i_max/4, 0, 2*3.14, sin);
        fill(current, 2, i_max/4, 0, 2*3.14, sin);
    }

    /* Minor Modifications */
    vectorAddTimer.start();

    ret = simulate(i_max, t_max, num_threads, old, current, next);
    
    vectorAddTimer.stop();

    cout << vectorAddTimer;

    file_write_double_array("result.txt", ret, i_max);
    /* End Minor Modifications */
    
    free(old);
    free(current);
    free(next);

    return EXIT_SUCCESS;
}
