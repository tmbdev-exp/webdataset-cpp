#include "webdataset.h"

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

namespace webdataset {

    using namespace std;

    using Stdio = std::shared_ptr<FILE>;
    using Tarfile = std::pair<std::string, std::string>;
    using Refill = void(*)(std::vector<std::string> &);

    Stdio gopen(const std::string &);

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

    class FileReader {
    private:
        Stdio stream;
        shared_ptr<Tarfile> item;
    public:
        FileReader() = default;
        void set_stream(Stdio stream) {
            this->stream = stream;
            item = nullptr;
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
                item->first = string(header.prefix) + string(header.name);
                int size = stoi(string(header.size, 12), nullptr, 8);
                int blocks = (size + 511) / 512;
                int rounded = blocks * 512;
                if(rounded > 0) {
                    item->second.resize(rounded, '_');
                    int n2 = fread((char *)&item->second[0], 1, rounded, stream.get());
                    if(n2 != rounded) throw bad_tar_format();
                    item->second.resize(size);
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


    class SampleReader {
    private:
        shared_ptr<FileReader> source;
        shared_ptr<Sample> item;
    public:
        SampleReader() = default;
        void set_source(shared_ptr<FileReader> source) {
            this->source = source;
            item = nullptr;
        }
        bool fetch() {
            item = nullptr;
            string key = "";
            for(;;) {
                auto file = source->peek();
                if(!file) return bool(item);
                auto [base, ext] = splitext(file->first);
                assert(base != "");
                if(key=="") {
                    key = base;
                    item = make_shared<Sample>();
                    (*item)["__key__"s] = key;
                }
                if(key!=base) {
                    return true;
                }
                (*item)[ext] = file->second;
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


    class WebDatasetReader : public IWebDatasetReader {
    private:
        vector<string> urls;
        string current_url;
        Stdio stream;
        shared_ptr<FileReader> files;
        shared_ptr<SampleReader> samples;
        function<void(vector<string> &)> refill = [](vector<string> &){};
    public:
        WebDatasetReader() = default;
        void add_url(const string &url) {
            urls.push_back(url);
        }
        void set_urls(const vector<string> &urls) {
            this->urls = urls;
            stream = nullptr;
            files = nullptr;
            samples = nullptr;
        }
        void set_refill(function<void(vector<string> &)> refill) {
            this->refill = refill;
        }
        bool next_url() {
            if(urls.size() == 0)
                refill(urls);
            if(urls.size() == 0) 
                return false;
            current_url = urls[0];
            urls.pop_back();
            stream = gopen(current_url);
            files.reset(new FileReader());
            files->set_stream(stream);
            samples.reset(new SampleReader());
            samples->set_source(files);
            return true;
        }
        bool forward() {
            while(!samples || !samples->peek()) {
                if(!next_url())
                    return  false;
            }
            return true;
        }
        shared_ptr<Sample> peek() {
            forward();
            return samples->peek();
        }
        shared_ptr<Sample> next() {
            forward();
            return samples->next();
        }
    };

    IWebDatasetReader *make_WebDatasetReader() {
        return new WebDatasetReader();
    }
}
