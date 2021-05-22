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

const regex splitext_re(R"(([^/.]*|.*/[^/.]*)(\.[^/]*|))");

class bad_tar_format : public exception {};
class short_tar_read : public exception {};

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
void print(const T &arg) {
    cout << arg << "\n";
}

template <class T, typename... Args>
void print(const T &arg, Args... args) {
    cout << arg << " ";
    print(args...);
}

void nsleep(double t) {
    int sec = floor(t);
    int nsec = 1e9*(t - sec);
    timespec ts{sec, nsec};
    nanosleep(&ts, 0);
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
        assert(stream);
        return Stdio(stream, pclose);
    }
    FILE *stream = fopen(fname.c_str(), "rb");
    assert(stream);
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

bool read_file(TarfileP &result, Stdio stream) {
    result = make_shared<Tarfile>();
    if(feof(stream.get())) return false;
    while(!feof(stream.get())) {
        posix_header header;
        int n1 = fread((char *)&header, 1, sizeof header, stream.get());
        if(n1 != sizeof header) throw bad_tar_format();
        result->key = string(header.prefix) + string(header.name);
        int size = stoi(string(header.size, 12), nullptr, 8);
        int blocks = (size + 511) / 512;
        int rounded = blocks * 512;
        if(rounded > 0) {
            result->value.resize(rounded, '_');
            int n2 = fread((char *)&result->value[0], 1, rounded, stream.get());
            if(n2 != rounded) throw bad_tar_format();
            result->value.resize(size);
        }
        if(header.typeflag == '0') return true;
    }
    return false;
}


auto splitext(string s) {
    auto groups = smatch{};
    assert(regex_match(s, groups, splitext_re));
    string base = groups[1];
    string ext = groups[2];
    return make_tuple(base, ext);
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

void harvest(vector<thread> &threads) {
    for (auto it = threads.begin(); it != threads.end(); ) {
        if(it->joinable()) {
            it->join();
            it = threads.erase(it);
        }
    }
}

class ThreadedReader {
public:
    vector<thread> readers;
    vector<thread> samplers;
    Tarfiles files{1000};
    Samples samples{1000};
    ThreadedReader(int nreaders, int nsamplers) {
        for(int i=0; i<nreaders; i++) {
            readers.push_back(thread(&ThreadedReader::reader, this));
        }
        for(int i=0; i<nsamplers; i++) {
            readers.push_back(thread(&ThreadedReader::sampler, this));
        }
    }
    void reader() {
        Stdio stream = gopen("imagenet-000000.tar");
        for(int i=0; i<100; i++) {
            TarfileP file = make_shared<Tarfile>();
            read_file(file, stream);
            files.push(file);
        }
        files.push(TarfileEOF());
    }
    void sampler() {
        for(;;) {
            SampleP sample = make_shared<Sample>();
            if(!getsample(*sample, files)) break;
            samples.push(sample);
        }
        samples.push(SampleEOF());
    }
    bool harvest() {
        ::harvest(readers);
        ::harvest(samplers);
        return readers.size() > 0 || samplers.size() > 0;
    }
    SampleP next() {
        for(;;) {
            harvest();
            SampleP sample;
            if(!samples.try_pop(sample)) {
                nsleep(0.1);
                continue;
            }
            return sample;
        }
    }
};

int main1(int argc, char **argv) {
    Tarfiles files(1000);
    Samples samples(1000);
    auto job1 = [&] {
        Stdio stream = gopen("imagenet-000000.tar");
        for(int i=0; i<100; i++) {
            TarfileP file = make_shared<Tarfile>();
            read_file(file, stream);
            files.push(file);
        }
        files.push(TarfileEOF());
    };
    auto job2 = [&] {
        for(;;) {
            SampleP sample = make_shared<Sample>();
            if(!getsample(*sample, files)) break;
            samples.push(sample);
        }
        samples.push(SampleEOF());
    };
    job1();
    job2();
    for(;;) {
        SampleP sample;
        if(!samples.try_pop(sample)) {
            nsleep(0.1);
            continue;
        }
        if(is_eof(*sample)) break;
        print((*sample)["__key__"s]);
        // for(auto [key, value] : *sample) { print(key); } 
    }
    //t1.join();
    //t2.join();
    return 0;
}

int main() {
    ThreadedReader reader(1, 1);
    for(;;) {
        SampleP sample = reader.next();
        if(is_eof(*sample)) break;
        print((*sample)["__key__"s]);
    }
    reader.harvest();
}
