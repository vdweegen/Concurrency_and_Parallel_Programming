/* Name: Cas van der Weegen & Alper Yerlibucak
 * Stdnr: 6055338 & 10219358
 * Date : 10/11/2013
 *
 * simulate.c
 *
 * Implement your (parallel) simulation here!
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "simulate.h"

struct wave_struct{
	double *next_array;
	double *current_array; 
	double *old_array;
	int i_part;
	int thread_start;
	int thread_end;
} ;

/* Add any global variables you may need. */
volatile int waiting = 1;
/*
 * Use three equal-sized buﬀers to store the three generations of the wave needed simultaneously.
* Rotate the buﬀers after each time step.
* Two initial generations of the wave shall be read in from ﬁle before starting the simulation, and
* the ﬁnal wave shall be written to ﬁle after completing the simulation. Parallelise your program by
* creating the given number of additional threads and let all threads collaboratively simulate each
* time step. Divide the work appropriately among the threads.
*/
void *berekening(void *arguments){
	struct wave_struct *wave = arguments;
	double c = 0.2;
	int j = wave->thread_end;
	int i = wave->thread_start;
	waiting = 1;
	for (; i < j; i++){
		wave->next_array[i] = 2 * wave->current_array[i] -
		wave->old_array[i] + c * (wave->old_array[i] - 
		(2 * wave->current_array[i] - wave->next_array[i] ));
	}
	return arguments;
}

double *simulate(const int i_max, const int t_max, const int num_threads,
        double *old_array, double *current_array, double *next_array){
	struct wave_struct wave;
	pthread_t thread_ids[num_threads];
	void *result;
	double *tmp;
	wave.i_part = i_max/num_threads;
	int over = i_max%num_threads;
	int j;
	int h;
	wave.old_array = old_array;
	wave.current_array = current_array;
	wave.next_array = next_array;
	for (j = 0; j < t_max; j++){
		/* Data may the altered after current thread has finished reading */
		while(!waiting);
		waiting = 0;
		h = 0;
		/* Spawn Threads */
		wave.thread_start = h * wave.i_part;
		wave.thread_end = (h+1) * wave.i_part + over;
		pthread_create ( &thread_ids [h] , NULL ,&berekening , &wave);
			
		for (h = 1; h < num_threads ; h++) {
			while(!waiting);
			waiting = 0;
			wave.thread_start = h * wave.i_part + over; 
			wave.thread_end = (h) * wave.i_part + over;
			pthread_create ( &thread_ids [h] , NULL ,&berekening , &wave);
		}
	
		for (int k = 0; k < num_threads; k++){
			pthread_join(thread_ids[k], &result);
		}
		/*
		* After each timestep, you should swap the buffers around. Watch out none
		* of the threads actually use the buffers at that time.
		*/	
		tmp = wave.old_array;
		wave.old_array = wave.current_array;
		wave.current_array = wave.next_array;
		wave.next_array = tmp;
	}
   /* You should return a pointer to the array with the final results. */
   return current_array;
}
