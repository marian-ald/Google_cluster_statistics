SOURCE=$(wildcard *.c)

TARGETS=$(patsubst %.c, %.run, $(SOURCE))

all: $(TARGETS)


%.run: %.c
	mpicc -o $@ $<

clean:
	rm -rf $(TARGETS)
