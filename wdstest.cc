#include "webdataset.h"
#include <iostream>
#include <string>

using namespace std;

namespace wds = webdataset;

template <class T>
void dprint(const T &arg) {
    cerr << arg << "\n";
}

template <class T, typename... Args>
void dprint(const T &arg, Args... args) {
    cerr << arg << " ";
    dprint(args...);
}

string url{"imagenet-000000.tar"};

int main() {
    unique_ptr<wds::IWebDatasetReader> wds;
    wds.reset(wds::make_WebDatasetReader());
    int countdown = 3;
    wds->set_refill([&](vector<string> &urls) {
        if(--countdown <= 0) return;
        dprint("refill");
        urls.push_back(url);
    });
    for(int i=0;; i++) {
        auto sample = wds->next();
        if(!sample) break;
        string keys;
        for(auto [k, v] : *sample) {
            keys += " ";
            keys += k;
        }
        dprint(i, (*sample)["__key__"], keys);
    }
}
