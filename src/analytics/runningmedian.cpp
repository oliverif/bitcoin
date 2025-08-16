#include <analytics/runningmedian.h>
#include <iterator>

RunningMedian::RunningMedian() {}

void RunningMedian::insert(double value) {
    if (window.empty()) {
        window.insert(value);
        median_it = window.begin();
        return;
    }

    window.insert(value);
    if (value < *median_it) {
        if (window.size() % 2 == 0) --median_it;
    } else {
        if (window.size() % 2 != 0) ++median_it;
    }
}

void RunningMedian::remove(double value) {
    if (window.empty()) return;

    if (value <= *median_it) {
        if (window.size() % 2 == 0) ++median_it;
    } else {
        if (window.size() % 2 != 0) --median_it;
    }

    auto it = window.find(value);
    if (it != window.end()) window.erase(it);
}

double RunningMedian::median() const {
    size_t n = window.size();
    if (n == 0) return 0.0;

    if (n % 2 == 1) {
        return *median_it;
    } else {
        auto prev = std::prev(median_it);
        return (*prev + *median_it) / 2.0;
    }
}

size_t RunningMedian::size() const {
    return window.size();
}

void RunningMedian::clear() {
    window.clear();
}