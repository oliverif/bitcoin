#include <analytics/runningmedian.h>
#include <iterator>

RunningMedian::RunningMedian() : median_it(window.end()) {}

void RunningMedian::init_from_data(const std::vector<double>& data)
{
    // Clear any existing data
    clear();

    if (data.empty()) {
        return;
    }

    // Insert all values into the multiset
    window.insert(data.begin(), data.end());

    // Set the median iterator to point to the correct position
    size_t n = window.size();
    if (n % 2 == 1) {
        // Odd number of elements: median is the middle element
        median_it = std::next(window.begin(), n / 2);
    } else {
        // Even number of elements: median_it points to the second of the two middle elements
        median_it = std::next(window.begin(), n / 2);
    }
}

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
