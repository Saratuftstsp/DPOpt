#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <stdexcept>

class GlobalStringEncoder {
public:
    // Encode or retrieve existing ID
    uint32_t encode(const std::string& s);

    // Retrieve string from ID
    const std::string& decode(uint32_t id) const;

    // Optional: freeze dictionary after preprocessing
    void freeze();

    // Size of dictionary
    size_t size() const;

private:
    std::unordered_map<std::string, uint32_t> forward_;
    std::vector<std::string> reverse_;
    bool frozen_ = false;
};


uint32_t GlobalStringEncoder::encode(const std::string& s)
{
    auto it = forward_.find(s);
    if (it != forward_.end())
        return it->second;

    if (frozen_)
        throw std::runtime_error("Encoder is frozen. Cannot insert new strings.");

    uint32_t id = static_cast<uint32_t>(reverse_.size());
    forward_[s] = id;
    reverse_.push_back(s);

    return id;
}

const std::string& GlobalStringEncoder::decode(uint32_t id) const
{
    if (id >= reverse_.size())
        throw std::out_of_range("Invalid string ID.");

    return reverse_[id];
}

void GlobalStringEncoder::freeze()
{
    frozen_ = true;
}

size_t GlobalStringEncoder::size() const
{
    return reverse_.size();
}

/*string filter_val = "JORDAN";
    uint32_t id = encoder.encode(filter_val);
*/