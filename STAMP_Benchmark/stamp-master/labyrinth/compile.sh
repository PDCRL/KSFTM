#!/bin/sh

cd ../../tl2;
rm libtl2.a stm.o;
g++ -pthread -std=c++11 -c stm.h stm.cpp;
ar -cvq libtl2.a stm.o;

cd ../stamp-master/lib;
rm *.o;
cd ../labyrinth;
rm labyrinth *.o;
make -f Makefile.stm;
