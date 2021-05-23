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

using namespace std;

using Stdio = shared_ptr<FILE>;

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
using Sample = map<string, string>;
using SampleP = shared_ptr<Sample>;

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

const regex splitext_re(R"(([^/.]*|.*/[^/.]*)(\.[^/]*|))");

auto splitext(string s) {
    auto groups = smatch{};
    assert(regex_match(s, groups, splitext_re));
    string base = groups[1];
    string ext = groups[2];
    return make_tuple(base, ext);
}

struct FileReader {
    Stdio stream;
    shared_ptr<Tarfile> item;
    FileReader(Stdio stream) : stream(stream) {
    }
    bool fetch() {
        item = nullptr;
        if(feof(stream.get())) return false;
        while(!feof(stream.get())) {
            posix_header header;
            int n1 = fread((char *)&header, 1, sizeof header, stream.get());
            if(n1 != sizeof header) throw bad_tar_format();
            if(header.typeflag == '\0') break;
            item = make_shared<Tarfile>();
            item->key = string(header.prefix) + string(header.name);
            int size = stoi(string(header.size, 12), nullptr, 8);
            int blocks = (size + 511) / 512;
            int rounded = blocks * 512;
            if(rounded > 0) {
                item->value.resize(rounded, '_');
                int n2 = fread((char *)&item->value[0], 1, rounded, stream.get());
                if(n2 != rounded) throw bad_tar_format();
                item->value.resize(size);
            }
            if(header.typeflag != '0') continue;
            return true;
        }
        return false;
    }
    shared_ptr<Tarfile> next() {
        if(!item) fetch();
        shared_ptr<Tarfile> result = item;
        fetch();
        return result;
    }
    shared_ptr<Tarfile> peek() {
        if(!item) fetch();
        return item;
    }
};

struct SampleReader {
    shared_ptr<FileReader> source;
    shared_ptr<Sample> item;
    SampleReader(shared_ptr<FileReader> source) : source(source) {
    }
    bool fetch() {
        item = nullptr;
        string key = "";
        for(;;) {
            auto file = source->peek();
            if(!file) return bool(item);
            auto [base, ext] = splitext(file->key);
            assert(base != "");
            if(key=="") {
                key = base;
                item = make_shared<Sample>();
                (*item)["__key__"s] = key;
            }
            if(key!=base) {
                return true;
            }
            (*item)[ext] = file->value;
            source->next();
        }
    }
    shared_ptr<Sample> next() {
        if(!item) fetch();
        shared_ptr<Sample> result = item;
        fetch();
        return result;
    }
    shared_ptr<Sample> peek() {
        if(!item) fetch();
        return item;
    }
};


string url{"imagenet-000000.tar"};

int main() {
    auto stream = gopen(url);
    shared_ptr<FileReader> files(new FileReader(stream));
    shared_ptr<SampleReader> samples(new SampleReader(files));
    for(int i=0;; i++) {
        auto sample = samples->next();
        if(!sample) break;
        string keys;
        for(auto [k, v] : *sample) {
            keys += " ";
            keys += k;
        }
        dprint(i, (*sample)["__key__"], keys);
    }
}
