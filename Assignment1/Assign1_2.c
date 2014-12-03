/*
 * Multithreaded prime number generator
 * With the algoritm of The Sieve of Eratosthenes
 *
 * names: Cas van der Weegen en Alper Yerlibucak
 * stdnum: 6055338 & 10219358
 *
 *
 */

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#define BUFSIZE 50

typedef struct {
    int data[BUFSIZE];
    int occupied;
    int nextin;
    int nextout;
    pthread_mutex_t *buflock;
    pthread_cond_t *objects;
    pthread_cond_t *room;
} Buffer;

void *prime_thread(void *args);

int main(){
    /* Set Variables */
    Buffer *buffer1;
    int val = 2;
    buffer1 = malloc(sizeof(Buffer));
    buffer1->occupied = 0;
    buffer1->nextin = 0;
    buffer1->nextout = 0;
    pthread_t next_thread;
	
    pthread_mutex_t block = PTHREAD_MUTEX_INITIALIZER;
    buffer1->buflock = &block;

    pthread_cond_t objects = PTHREAD_COND_INITIALIZER;
    buffer1->objects = &objects;

    pthread_cond_t room = PTHREAD_COND_INITIALIZER;
    buffer1->room = &room;

	/* Spawn new thread */
    pthread_create(&next_thread, NULL, prime_thread, (void *)buffer1);
    do{
        pthread_mutex_lock(buffer1->buflock);
        while(buffer1->occupied == BUFSIZE){
            pthread_cond_wait(buffer1->room, buffer1->buflock);
        }

        buffer1->data[buffer1->nextin] = val;
        buffer1->nextin = (buffer1->nextin + 1) % BUFSIZE;
        buffer1->occupied = buffer1->occupied +1;

        /* Unlock buffer */
    	  pthread_mutex_unlock(buffer1->buflock);
        pthread_cond_signal(buffer1->objects);
        
        val = val + 1;
    }while(1);
    return 0;
}

/* Thread filter, check for prime */
void *prime_thread(void *args)
{
    /* Initialize Variables */		
	int prime = 0;
	int next = 0;
	pthread_t next_thread;
	Buffer *buffer1, *buffer2;
	buffer1 = malloc(sizeof(Buffer));
	buffer2 = (Buffer *) args;
	buffer1 = malloc(sizeof(Buffer));

	pthread_mutex_t block = PTHREAD_MUTEX_INITIALIZER;
	buffer1->buflock = &block;

	pthread_cond_t objects = PTHREAD_COND_INITIALIZER;
	buffer1->objects = &objects;

	pthread_cond_t room = PTHREAD_COND_INITIALIZER;
	buffer1->room = &room;

    do{  
		/* Lock buffer while using */
        int number;
        pthread_mutex_lock(buffer2->buflock);
        
        while(buffer2->occupied == 0){
            pthread_cond_wait(buffer2->objects, buffer2->buflock);
        }

        if(prime == 0){
            prime = buffer2->data[buffer2->nextout];
            buffer2->nextout = (buffer2->nextout + 1) % BUFSIZE;
            buffer2->occupied =  buffer2->occupied - 1;
            /* Found Prime? Print */
            printf("[%d]", prime);
            /* Unlock buffer */
            pthread_cond_signal(buffer2->room);
            pthread_mutex_unlock(buffer2->buflock);
            continue;
        }

        number = buffer2->data[buffer2->nextout];
        buffer2->nextout = (buffer2->nextout + 1) % BUFSIZE;
        buffer2->occupied =  buffer2->occupied - 1;

        pthread_cond_signal(buffer2->room);
        pthread_mutex_unlock(buffer2->buflock);
        if(number % prime == 0){
            continue;
        }

        if(next == 0){
            /* Spawn next thread */
            pthread_create(&next_thread, NULL, &prime_thread,
                    (void *)buffer1);
            next = 1;
        }

        pthread_mutex_lock(buffer1->buflock);
        while(buffer1->occupied == BUFSIZE){
            pthread_cond_wait(buffer1->room, buffer1->buflock);
        }

        buffer1->data[buffer1->nextin] = number;
    	  buffer1->nextin = (buffer1->nextin + 1) % BUFSIZE;
        buffer1->occupied = buffer1->occupied +1;
        /* Unlock buffer */
        pthread_cond_signal(buffer1->objects);
        pthread_mutex_unlock(buffer1->buflock);
    }while(1);
}