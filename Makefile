all:
	g++ -g -std=c++17 -o webdataset webdataset.cc -lpthread
	./webdataset
