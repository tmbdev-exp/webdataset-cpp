#include <stdio.h>
#include <vector>
#include <map>
#include <string>
#include <exception>
#include <memory>
#include <functional>

namespace webdataset {

    class webdataset_error : public std::exception {};
    class bad_tar_format : public webdataset_error {};
    class short_tar_read : public webdataset_error {};
    class gopen_err : public webdataset_error {};

    using Sample = std::map<std::string, std::string>;

    class IWebDatasetReader {
    public:
        virtual void add_url(const std::string &) = 0;
        virtual void set_urls(const std::vector<std::string> &) = 0;
        virtual void set_refill(std::function<void(std::vector<std::string> &)>) = 0;
        virtual std::shared_ptr<Sample> peek() = 0;
        virtual std::shared_ptr<Sample> next() = 0;
    };

    IWebDatasetReader *make_WebDatasetReader();

}
