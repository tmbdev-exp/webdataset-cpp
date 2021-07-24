#include <unistd.h>
#include <assert.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

#include <iostream>
#include <chrono>
#include <set>
#include <fstream>
#include <string>
#include <exception>
#include <queue>
#include <deque>
#include <regex>
#include <thread>
#include <memory>

#include "MPMCQueue.h"

#include "webdataset.h"

using namespace std;
namespace wds = webdataset;
namespace chrono = std::chrono;
using duration = std::chrono::duration<double>;

template <class T>
void dprint(const T &arg) {
    cerr << arg << "\n";
}

template <class T, typename... Args>
void dprint(const T &arg, Args... args) {
    cerr << arg << " ";
    dprint(args...);
}

using Stdio = shared_ptr<FILE>;

template <class T>
using Channel = rigtorp::MPMCQueue<T>;
template <class T>
using ChannelP = shared_ptr<Channel<T>>;

void nsleep(double t) {
    int sec = floor(t);
    int nsec = 1e9*(t - sec);
    timespec ts{sec, nsec};
    nanosleep(&ts, 0);
}

template <class IN, class OUT>
class BaseProcessor {
public:
    void add(IN &in) {
        while(running && !inch->try_push(move(in))) {
            nsleep(wait);
        }
    }
    bool get(OUT &out, double timeout=1e33) {
        auto start = chrono::steady_clock::now();
        while(running) {
            if(outch->try_pop(out))
                return true;
            auto end = chrono::steady_clock::now();
            duration delta = end - start;
            if(delta.count() >= timeout)
                return false;
            nsleep(wait);
        }
        return false;
    }
    void start(int nthread) {
        for(int i=0; i<nthread; i++) {
            jobs.push_back(thread(&BaseProcessor::loop, this));
        }
    }
    void finish() {
        running = false;
        for(int i=0; i<jobs.size(); i++) {
            jobs[i].join();
        }
    }
    virtual void loop() = 0;
protected:
    bool running = true;
    double wait = 0.01;
    ChannelP<IN> inch{new Channel<IN>(100)};
    ChannelP<OUT> outch{new Channel<OUT>(100)};
    vector<thread> jobs;
    bool recv(IN &in) {
        while(running) {
            if(inch->try_pop(in))
                return true;
            nsleep(wait);
        }
        return false;
    }
    bool send(OUT &out) {
        while(running) {
            if(outch->try_push(out))
                return true;
            nsleep(wait);
        }
        return false;
    }
};


class DatasetReader : public BaseProcessor<string, shared_ptr<wds::Sample>> {
public:
    DatasetReader() {
        wds.reset(wds::make_WebDatasetReader());
    }
private:
    unique_ptr<wds::IWebDatasetReader> wds;
    void loop() {
        while(running) {
            string in;
            if(!recv(in)) break;
            dprint("URL", in);
            wds->add_url(in);
            for(;;) {
                shared_ptr<wds::Sample> sample = wds->next();
                if(!sample) break;
                if(!send(sample)) break;
            }
        }
    }
};

template <class IN, class OUT>
class MapProcessor : public BaseProcessor<IN, OUT> {
public:
    auto with(function<OUT(*)(IN)> f) {
        this->f = f;
        return *this;
    }
private:
    function<OUT(*)(IN)> f;
    void loop() {
        while(this->running) {
            IN in;
            recv(in);
            OUT out = f(in);
            send(out);
        }
    }
};

string url{"imagenet-000000.tar"};

int main() {
    DatasetReader dsr;
    shared_ptr<wds::Sample> sample;
    dsr.start(1);
    dsr.add(url);
    while(dsr.get(sample, 2.0)) {
        dprint((*sample)["__key__"]);
    }
    dsr.finish();
}
