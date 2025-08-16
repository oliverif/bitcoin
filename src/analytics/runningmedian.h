#pragma once

#include <set>

class RunningMedian {
public:
    RunningMedian();

    void insert(double value);
    void remove(double value);
    double median() const;
    size_t size() const;
    void clear();

private:
    std::multiset<double> window;
    std::multiset<double>::iterator median_it;
};