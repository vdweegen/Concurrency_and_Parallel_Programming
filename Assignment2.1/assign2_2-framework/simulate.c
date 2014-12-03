/*
 * simulate.c
 *
 * Implement your (parallel) simulation here!
 */

#include <stdio.h>
#include <stdlib.h>

#include "omp.h"
#include "simulate.h"

double sum(double* vec, int len){
	int i; double accu = 0;
    #pragma omp parallel for
    for (i=0; i<len; i++) {
        accu = accu + vec[i];
    }
    return accu;
}

double reduce (double fun(double, double), double * vec , int len){
	int i; double accu = 1;
	#pragma omp parallel for
	for (i=0; i<len; i++){
		accu = fun(accu , vec[i]);
	}	
	return accu;
}

double function(double x, double y){
	return x*y;
}

double *simulate(const int i_max, const int t_max, const int num_threads,
        double *old_array, double *current_array, double *next_array){
	int x = 1000000;
	double *vec = (double *) malloc(x* sizeof(double));
   double (*fun)(double, double) = &function;
	int i;
   for (i = 0; i < x; i++){
		vec[i] = vec[i] + i;
	}
	double sumval, reduceval;
	reduceval = reduce(fun, vec, x);
	sumval = sum(vec, x);
   printf("Sum: %f\n", sumval);
	printf("Reduce: %f\n", reduceval);
    
   return current_array;
}



