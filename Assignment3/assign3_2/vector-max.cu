#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "timer.h"
#include <iostream>

using namespace std;

/* Utility function, use to do error checking.

   Use this function like this:

   checkCudaCall(cudaMalloc((void **) &deviceRGB, imgS * sizeof(color_t)));

   And to check the result of a kernel invocation:

   checkCudaCall(cudaGetLastError());
*/
static void checkCudaCall(cudaError_t result) {
    if (result != cudaSuccess) {
        cerr << "cuda error: " << cudaGetErrorString(result) << endl;
        exit(1);
    }
}

__global__ void vectorReduceKernel(int n, float* deviceA, float* deviceResult){
   unsigned index = blockIdx.x * blockDim.x + threadIdx.x;
   
    __shared__ double sf[1024];
    sf[threadIdx.x] = deviceA[index];
    __syncthreads();
    
    // do comparison, copy biggest to result array
    if ((index % 2 == 0) && index < n) {
        if (sf[index] > sf[index+1]){
            deviceResult[index/2] = sf[index];
        }else{
            deviceResult[index/2] = sf[index+1];    
        }
    }
}


void vectorAddCuda(int n, float* a, float* result) {
    int threadBlockSize = 512;

    // allocate the vectors on the GPU
    float* deviceA = NULL;
    checkCudaCall(cudaMalloc((void **) &deviceA, n * sizeof(float)));
    if (deviceA == NULL) {
        cout << "could not allocate memory!" << endl;
        return;
    }
    float* deviceResult = NULL;
    checkCudaCall(cudaMalloc((void **) &deviceResult, n * sizeof(float)));
    if (deviceResult == NULL) {
        checkCudaCall(cudaFree(deviceA));
        cout << "could not allocate memory!" << endl;
        return;
    }

    cudaEvent_t start, stop;
    cudaEventCreate(&start);
    cudaEventCreate(&stop);

    // copy the original vectors to the GPU
    checkCudaCall(cudaMemcpy(deviceA, a, n*sizeof(float), cudaMemcpyHostToDevice));

    // execute kernel
    cudaEventRecord(start, 0);
    vectorReduceKernel<<<n/threadBlockSize, threadBlockSize>>>(n, deviceA, deviceResult);
    cudaEventRecord(stop, 0);

    // check whether the kernel invocation was successful
    checkCudaCall(cudaGetLastError());

    // copy result back
    checkCudaCall(cudaMemcpy(result, deviceResult, n * sizeof(float), cudaMemcpyDeviceToHost));

    checkCudaCall(cudaFree(deviceA));
    checkCudaCall(cudaFree(deviceResult));

    // print the time the kernel invocation took, without the copies!
    float elapsedTime;
    cudaEventElapsedTime(&elapsedTime, start, stop);
    
    cout << "kernel invocation took " << elapsedTime << " milliseconds" << endl;
}


int main(int argc, char* argv[]) {
    int n = 1024;
    timer vectorAddTimer("vector add timer");
    float* a = new float[n];
    float* result = new float[n];

    // initialize the vectors.
    for(int i=0; i<n; i++) {
        a[i] = (float)rand()/(float)RAND_MAX;
    }

    vectorAddTimer.start();
    vectorAddCuda(n, a, result);
    vectorAddTimer.stop();

    cout << vectorAddTimer;

    // verify the resuls
    for(int i=0; i<n; i++) {
        if(result[i] != 2*i) {
            cout << "error in results! Element " << i << " is " << result[i] << ", but should be " << (2*i) << endl;
            exit(1);
        }
    }
    cout << "results OK!" << endl;
            
    delete[] a;
    delete[] result;
    
    return 0;
}
