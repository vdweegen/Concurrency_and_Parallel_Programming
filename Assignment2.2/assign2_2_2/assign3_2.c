#include <stdio.h>
#include <string.h>
#include "mpi.h"




int MYMPI_Bcast(void *buffer, int count, MPI_Datatype datatype, 
	int root, MPI_Comm communicator){
	int my_rank, size, tag = 0;
	MPI_Status status;

	MPI_Comm_rank(communicator, &my_rank); // task id
	MPI_Comm_size(communicator, &size); //size

	if (my_rank == root) { // if root -> start sending
		MPI_Send(buffer, count, datatype, my_rank+1 , tag, communicator);
	
	} else { // if not root -> recieve msg and pass on message to next neighbour
		MPI_Recv(buffer, count, datatype, my_rank-1 , tag, communicator, &status);
		if (my_rank+1 < size) {
			MPI_Send(buffer, count, datatype, my_rank+1, tag, communicator);
		}
	}
	return MPI_SUCCESS;

}




int main (int argc, char *argv[]) {
	int rc, num_tasks, my_rank, root = 0;
	char buffer[32];

	rc = MPI_Init(&argc, &argv);
	if (rc != MPI_SUCCESS) {
		fprintf(stderr, "Unable to set up MPI\n");
		MPI_Abort(MPI_COMM_WORLD, rc);
	}

	MPI_Comm_size(MPI_COMM_WORLD, &num_tasks); // number of tasks
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); // task id

	if (my_rank == root) { // if root -> start broadcasting
		strcpy(buffer, "Alper is Awesome!");
		printf("I'm task number %d of a total of %d tasks, broadcasting: \"%s\"\n\n",my_rank, num_tasks, buffer);
		MYMPI_Bcast(&buffer, strlen(buffer)+1, MPI_CHAR, root, MPI_COMM_WORLD);
	
	} else { // if not root -> recieve broadcast
		MYMPI_Bcast(&buffer, 32, MPI_CHAR, my_rank-1, MPI_COMM_WORLD);
		printf("I'm task number %d and I recieved: \"%s\" from task %d\n\n",my_rank, buffer, my_rank-1);
	}

	MPI_Finalize(); // Shutdown MPI runtime
}