/* Name: Cas van der Weegen & Alper Yerlibucak
 * Stdnr: 6055338 & 10219358
 * Date : 26/11/2013
 * 
 * simulate.c
 *
 * Implement your (parallel) simulation here!
 */

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>

#include "simulate.h"
#include "mpi.h"


/* Add any global variables you may need. */


/* Add any functions you may need (like a worker) here. */


/*
 * Executes the entire simulation.
 *
 * Implement your code here.
 *
 * i_max: how many data points are on a single wave
 * t_max: how many iterations the simulation should run
 * old_array: array of size i_max filled with data for t-1
 * current_array: array of size i_max filled with data for t
 * next_array: array of size i_max. You should fill this with t+1
 */
double *simulate(int argc, char * argv[], const int i_max, const int t_max, double *old_array, double *current_array, double *next_array){
	int rc,
			num_tasks,
			my_rank,
			left_neighbour,
			right_neighbour;
	double c = 0.2;
		
	rc = MPI_Init(&argc, &argv);
	
	// Check for success
	if(rc != MPI_SUCCESS){ 
		fprintf(stderr, "Unable to set up MPI");
		MPI_Abort(MPI_COMM_WORLD, rc);
	}

    MPI_Status status;
	MPI_Comm_size(MPI_COMM_WORLD, &num_tasks); // number of tasks
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); // task id
	
	left_neighbour = my_rank - 1;
	right_neighbour = my_rank + 1;
	int	local_size = (i_max/num_tasks);
	double *old = calloc(local_size + 2, sizeof(double));
	double *new = calloc(local_size + 2, sizeof(double));
	double *current = calloc(local_size + 2, sizeof(double));

	if(my_rank == 0){
		/* If head node, send out data */
			memcpy(current + 1, current_array, (sizeof(double) * local_size));
			memcpy(old + 1, old_array, (sizeof(double) * local_size));
			for (int t = 1; t < num_tasks; t++){
				MPI_Send(old_array + (t * local_size), local_size, MPI_DOUBLE, t, 0, MPI_COMM_WORLD);
				MPI_Send(current_array + (t * local_size), 1, MPI_DOUBLE, t, 1, MPI_COMM_WORLD);
			}
		}else{
		/* If not Head Node, recieve the data */
			MPI_Recv(old + 1, local_size, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, &status);
			MPI_Recv(current + 1, local_size, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD, &status);
		}
	
	for(int t = 0; t <= local_size; t++){
		/* Send data to the left if not Head Note */
		if(my_rank > 0){
			MPI_Send(current + 1, 1, MPI_DOUBLE, left_neighbour, t, MPI_COMM_WORLD);
		}
		/* Receive data from the right if not last process/node */
		if(my_rank != (num_tasks - 1)){
			MPI_Send(current + local_size, 1 , MPI_DOUBLE , right_neighbour , t , MPI_COMM_WORLD);
			MPI_Recv(current + local_size + 1, 1 , MPI_DOUBLE , right_neighbour, t , MPI_COMM_WORLD , &status );
		}
		
		/* If not head node, get data an process */
		if(my_rank > 0){
			MPI_Recv(current, 1 , MPI_DOUBLE , left_neighbour, t , MPI_COMM_WORLD , &status);
		  for (int x = 1; x <= local_size; x++){
		    new[x] = 2.0 * current[x] - old[x] + c * (current[x - 1] - (2 * current[x] - current[x + 1]));
		  }
			/* Store data and Move on */
		  old = current;
		  current = new;
		}
	}
	
	/* If not head node, finalize */
	if(my_rank > 0){
		MPI_Send(current + 1, local_size, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
		MPI_Finalize();
		exit(0);
	/* Else, merge data and return */
	}else{
		memcpy(current_array, current+1, sizeof(double));
		for(int x = 1; x < num_tasks; x++){
				MPI_Recv(current_array + (x * local_size), local_size, MPI_DOUBLE, x, 0, MPI_COMM_WORLD, &status);
		}
	}
	free(old);
	free(new);
	free(current);
	
    /* You should return a pointer to the array with the final results. */
    MPI_Finalize();
    return current_array;
}
