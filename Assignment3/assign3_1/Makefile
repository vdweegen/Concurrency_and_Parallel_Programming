NVCC		= nvcc
CC		= g++
#CU_FLAGS	= -O3 -g -deviceemu
CU_FLAGS	= -O3 -g --ptxas-options=-v -arch compute_20 -code sm_20
CC_FLAGS	= -O3 -m64 -Wall
PROGNAME = simulate

IMAGEVIEW = display
RUNARGS = 1000 1000 8 sinfull

CU_SOURCES	= simulate.cu
CC_SOURCES	= timer.cc file.cc

CU_OBJECTS	= $(CU_SOURCES:%.cu=%.o)
CU_PTX		= $(CU_SOURCES:%.cu=%.ptx)
CC_OBJECTS	= $(CC_SOURCES:%.cc=%.o)

%.o:		%.cu
		$(NVCC) $(CU_FLAGS) -c $< -o $@

%.o:		%.cc
		$(CC) $(CC_FLAGS) -c $< -o $@

%.ptx:		%.cu
		$(NVCC) $(CU_FLAGS) --ptx $< -o $@

simulate:	$(CU_OBJECTS) $(CC_OBJECTS)
		$(NVCC) $^ -o $@

run: $(PROGNAME)
	prun -v -np 1 -native '-l gpu=GTX480' $(PROGNAME) $(RUNARGS)

ptx:		$(CU_PTX)

plot: result.txt
	gnuplot plot.gnp
	$(IMAGEVIEW) plot.png

clean:
		rm -f *.o $(PROGNAME) *~
