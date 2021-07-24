webproc: webproc.cc webdataset.cc
	g++ -g -std=c++17 -o webproc webproc.cc webdataset.cc -lpthread
	./webproc

wdstest: wdstest.cc webdataset.cc
	g++ -g -std=c++17 -o wdstest wdstest.cc webdataset.cc -lpthread
	./wdstest
