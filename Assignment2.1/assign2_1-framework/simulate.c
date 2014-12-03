/*
 * simulate.c
 *
 * Implement your (parallel) simulation here!
 */

#include <stdio.h>
#include <stdlib.h>
#include "omp.h"
#include "simulate.h"

/*
 * Executes the entire simulation.
 *
 * Implement your code here.
 *
 * i_max: how many data points are on a single wave
 * t_max: how many iterations the simulation should run
 * num_threads: how many threads to use
 * old_array: array of size i_max filled with data for t-1
 * current_array: array of size i_max filled with data for t
 * next_array: array of size i_max. You should fill this with t+1
 */
double *simulate(const int i_max, const int t_max, const int num_threads,
        double *old_array, double *current_array, double *next_array)
{
   omp_set_num_threads(num_threads);
	printf("number of threads: %d\n", num_threads);
	fflush(stdout);

   for (int t = 2; t <= t_max; t++) {
      double *temp;

      # pragma omp parallel for
      for(int i = 1; i < i_max - 1; i++){
         next_array[i] = 2 * current_array[i] - old_array[i] + 0.2 *
			(current_array[i-1] - (2 * current_array[i] - current_array[i + 1]));
      }

      temp = old_array;
      old_array = current_array;
      current_array = next_array;
      next_array = temp;
   }
   return current_array;
}
