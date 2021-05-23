#include <unistd.h>
#include <assert.h>
#include <stdlib.h>
#include <math.h>

#include <iostream>
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

using namespace std;

using Stdio = shared_ptr<FILE>;

template <class T>
using Channel = rigtorp::MPMCQueue<T>;

using Sources = Channel<string>;

const regex splitext_re(R"(([^/.]*|.*/[^/.]*)(\.[^/]*|))");

class bad_tar_format : public exception {};
class short_tar_read : public exception {};
class gopen_err : public exception {};

struct posix_header {           /* byte offset */
  char name[100];               /*   0 */
  char mode[8];                 /* 100 */
  char uid[8];                  /* 108 */
  char gid[8];                  /* 116 */
  char size[12];                /* 124 */
  char mtime[12];               /* 136 */
  char chksum[8];               /* 148 */
  char typeflag;                /* 156 */
  char linkname[100];           /* 157 */
  char magic[6];                /* 257 */
  char version[2];              /* 263 */
  char uname[32];               /* 265 */
  char gname[32];               /* 297 */
  char devmajor[8];             /* 329 */
  char devminor[8];             /* 337 */
  char prefix[155];             /* 345 */
                                /* 500 */
  char pad[12];
};

struct Tarfile {
    string key;
    string value;
    Tarfile() noexcept {}
    Tarfile(const string &key, const string &value) : key(key), value(value) {}
    Tarfile(const Tarfile &other) noexcept {
        key = other.key;
        value = other.value;
    }
    void operator=(const Tarfile &other) noexcept {
        key = other.key;
        value = other.value;
    }
};

using TarfileP = shared_ptr<Tarfile>;
using Tarfiles = Channel<TarfileP>;


using Sample = map<string, string>;
using SampleP = shared_ptr<Sample>;
using Samples = Channel<SampleP>;

template <class T>
void dprint(const T &arg) {
    cerr << arg << "\n";
}

template <class T, typename... Args>
void dprint(const T &arg, Args... args) {
    cerr << arg << " ";
    dprint(args...);
}

void nsleep(double t) {
    int sec = floor(t);
    int nsec = 1e9*(t - sec);
    timespec ts{sec, nsec};
    nanosleep(&ts, 0);
}

void harvest(vector<thread> &threads) {
    for (auto it = threads.begin(); it != threads.end(); ) {
        if(it->joinable()) {
            it->join();
            it = threads.erase(it);
        }
    }
}

string quote(string s) {
    string result;
    for(int i=0; i<s.size(); i++) {
        char c = s[i];
        if(isprint(c)) {
            result += c;
        } else {
            result.append("?");
        }
    }
    return result;
}

Stdio gopen(const string &fname) {
    if(fname.find("pipe:") == 0) {
        FILE *stream = popen(fname.c_str(), "rb");
        if(!stream) throw gopen_err();
        return Stdio(stream, pclose);
    }
    FILE *stream = fopen(fname.c_str(), "rb");
    if(!stream) throw gopen_err();
    return Stdio(stream, fclose);
}

TarfileP TarfileEOF() {
    auto result = make_shared<Tarfile>();
    result->key = "__EOF__";
    return result;
}

bool is_eof(Tarfile &tf) {
    return tf.key == "__EOF__";
}


SampleP SampleEOF() {
    auto result = make_shared<Sample>();
    (*result)["__key__"] = "__EOF__";
    return result;
}

bool is_eof(Sample &sample) {
    return sample["__key__"] == "__EOF__";
}

auto splitext(string s) {
    auto groups = smatch{};
    assert(regex_match(s, groups, splitext_re));
    string base = groups[1];
    string ext = groups[2];
    return make_tuple(base, ext);
}

bool read_file(TarfileP &result, Stdio stream) {
    result = make_shared<Tarfile>();
    if(feof(stream.get())) return false;
    while(!feof(stream.get())) {
        posix_header header;
        int n1 = fread((char *)&header, 1, sizeof header, stream.get());
        if(n1 != sizeof header) throw bad_tar_format();
        if(header.typeflag == '\0') break;
        result->key = string(header.prefix) + string(header.name);
        // print("#", header.typeflag, quote(string(header.size, 12)), result->key);
        int size = stoi(string(header.size, 12), nullptr, 8);
        int blocks = (size + 511) / 512;
        int rounded = blocks * 512;
        if(rounded > 0) {
            result->value.resize(rounded, '_');
            int n2 = fread((char *)&result->value[0], 1, rounded, stream.get());
            if(n2 != rounded) throw bad_tar_format();
            result->value.resize(size);
        }
        if(header.typeflag != '0') continue;
        return true;
    }
    return false;
}

bool getsample(Sample &sample, Tarfiles &source) {
    sample.clear();
    string key = "";
    for(;;) {
        TarfileP file;
        source.pop(file);
        if(is_eof(*file)) {
            return (sample.size() == 0);
        }
        auto [base, ext] = splitext(file->key);
        assert(base != "");
        if(key=="") {
            key = base;
            sample["__key__"s] = key;
        }
        if(key!=base) {
            return true;
        }
        sample[ext] = file->value;
    }
}

class ThreadedReader {
public:
    atomic<int> processed = 0;
    bool running = true;
    Sources sources{10000};
    Tarfiles files{1000};
    Samples samples{1000};
    vector<thread> readers;
    vector<thread> samplers;
    void add_source(string source) {
        sources.push(move(source));
    }
    void start(int nreaders, int nsamplers) {
        for(int i=0; i<nreaders; i++) {
            readers.push_back(thread(&ThreadedReader::reader, this));
        }
        for(int i=0; i<nsamplers; i++) {
            readers.push_back(thread(&ThreadedReader::sampler, this));
        }
    }
    void reader() {
        string source;
        while(running) {
            sources.pop(source);
            Stdio stream = gopen(source);
            for(;;) {
                TarfileP file = make_shared<Tarfile>();
                if(!read_file(file, stream)) break;
                files.push(file);
            }
            processed += 1;
        }
    }
    void sampler() {
        while(running) {
            SampleP sample = make_shared<Sample>();
            getsample(*sample, files);
            samples.push(sample);
        }
    }
    SampleP next() {
        for(;;) {
            SampleP sample;
            if(!samples.try_pop(sample)) {
                nsleep(0.1);
                continue;
            }
            return sample;
        }
    }
    bool done() {
    }
    bool harvest() {
        ::harvest(readers);
        ::harvest(samplers);
        return readers.size() > 0 || samplers.size() > 0;
    }
    void close() {
        running = false;
        while(harvest()) {
            nsleep(0.3);
        }
    }
};

int main() {
    ThreadedReader reader;
    reader.start(1, 1);
    reader.add_source("imagenet-000000.tar");
    for(;;) {
        SampleP sample = reader.next();
        if(is_eof(*sample)) break;
        dprint((*sample)["__key__"s]);
    }
    reader.close();
}
