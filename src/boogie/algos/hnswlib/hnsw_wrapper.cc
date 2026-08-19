// hnsw_wrapper.cpp
#include <iostream>
#include "hnswlib/hnswlib.h"
#include "hnsw_wrapper.h"
#include <thread>
#include <atomic>
#include <vector>


static std::vector<std::vector<float>> convertTo2DVector(const float* flat_vectors, int rows, int cols);

/*
 * replacement for the openmp '#pragma omp parallel for' directive
 * only handles a subset of functionality (no reductions etc)
 * Process ids from start (inclusive) to end (EXCLUSIVE)
 *
 * The method is borrowed from nmslib
 */
template <class Function>
inline void ParallelFor(size_t start, size_t end, size_t numThreads, Function fn)
{
    if (numThreads <= 0)
    {
        numThreads = std::thread::hardware_concurrency();
    }

    if (numThreads == 1)
    {
        for (size_t id = start; id < end; id++)
        {
            fn(id, 0);
        }
    }
    else
    {
        std::vector<std::thread> threads;
        std::atomic<size_t> current(start);

        // keep track of exceptions in threads
        // https://stackoverflow.com/a/32428427/1713196
        std::exception_ptr lastException = nullptr;
        std::mutex lastExceptMutex;

        for (size_t threadId = 0; threadId < numThreads; ++threadId)
        {
            threads.push_back(std::thread([&, threadId]
                                          {
                while (true) {
                    size_t id = current.fetch_add(1);

                    if (id >= end) {
                        break;
                    }

                    try {
                        fn(id, threadId);
                    } catch (...) {
                        std::unique_lock<std::mutex> lastExcepLock(lastExceptMutex);
                        lastException = std::current_exception();
                        /*
                         * This will work even when current is the largest value that
                         * size_t can fit, because fetch_add returns the previous value
                         * before the increment (what will result in overflow
                         * and produce 0 instead of current + 1).
                         */
                        current = end;
                        break;
                    }
                } }));
        }
        for (auto &thread : threads)
        {
            thread.join();
        }
        if (lastException)
        {
            std::rethrow_exception(lastException);
        }
    }
}

class CustomFilterFunctor : public hnswlib::BaseFilterFunctor
{
    std::function<bool(hnswlib::labeltype)> filter;

public:
    explicit CustomFilterFunctor(const std::function<bool(hnswlib::labeltype)> &f)
    {
        filter = f;
    }

    bool operator()(hnswlib::labeltype id)
    {
        return filter(id);
    }
};

HnswIndex *newIndex(spaceType space_type, const int dim, size_t max_elements, int M, int ef_construction, int rand_seed, int allow_replace_deleted)
{
    HnswIndex *index = new HnswIndex;
    bool normalize = false;
    hnswlib::SpaceInterface<float> *space;
    if (space_type == l2)
    {
        space = new hnswlib::L2Space(dim);
    }
    else if (space_type == ip)
    {
        space = new hnswlib::InnerProductSpace(dim);
    }
    else if (space_type == cosine)
    {
        space = new hnswlib::InnerProductSpace(dim);
        normalize = true;
    }
    else
    {
        throw std::runtime_error("Space name must be one of l2, ip, or cosine.");
    }

    hnswlib::HierarchicalNSW<float> *appr_alg = new hnswlib::HierarchicalNSW<float>(space, max_elements, M, ef_construction, rand_seed, static_cast<bool>(allow_replace_deleted));

    index->hnsw = (void *)appr_alg;
    index->dim = dim;
    index->normalize = normalize;
    index->space = (void *)space;
    index->space_type = space_type;
    return index;
}

// set efConstruction value.
void setEf(HnswIndex *index, size_t ef)
{
    ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->ef_ = ef;
}

// Returns index file size in size_t.
size_t indexFileSize(HnswIndex *index)
{
    return ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->indexFileSize();
}

// Save index to a file.
void saveIndex(HnswIndex *index, char *location)
{
    ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->saveIndex(location);
}

HnswIndex *loadIndex(char *location, spaceType space_type, int dim, size_t max_elements, int allow_replace_deleted)
{
    HnswIndex *index = new HnswIndex;
    bool normalize = false;
    hnswlib::SpaceInterface<float> *space;
    if (space_type == l2)
    {
        space = new hnswlib::L2Space(dim);
    }
    else if (space_type == ip)
    {
        space = new hnswlib::InnerProductSpace(dim);
    }
    else if (space_type == cosine)
    {
        space = new hnswlib::InnerProductSpace(dim);
        normalize = true;
    }
    else
    {
        throw std::runtime_error("Space name must be one of l2, ip, or cosine.");
    }

    hnswlib::HierarchicalNSW<float> *appr_alg = new hnswlib::HierarchicalNSW<float>(space, location, false, max_elements, static_cast<bool>(allow_replace_deleted));

    index->hnsw = (void *)appr_alg;
    index->dim = dim;
    index->normalize = normalize;
    index->space = (void *)space;
    index->space_type = space_type;
    return index;
}

void normalize_vector(int dim, float *data, float *norm_array)
{
    float norm = 0.0f;
    for (int i = 0; i < dim; i++)
        norm += data[i] * data[i];
    norm = 1.0f / (sqrtf(norm) + 1e-30f);
    for (int i = 0; i < dim; i++)
        norm_array[i] = data[i] * norm;
}

int addPoints(HnswIndex *index, const float *flat_vectors, int rows, size_t *labels, int num_threads, int replace_deleted)
{
    // avoid using threads when the number of additions is small:
    if (rows <= num_threads * 4)
    {
        num_threads = 1;
    }

    std::vector<std::vector<float>> vectors = convertTo2DVector(flat_vectors, rows, index->dim);

    try {
        if (index->normalize == false) {
            ParallelFor(0, rows, num_threads, [&](size_t row, size_t threadId) {
                size_t id = *(labels + row);
                ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->addPoint(vectors[row].data(), id, static_cast<bool>(replace_deleted));
            });
            return 0;
        }

        std::vector<float> norm_array(num_threads * (index->dim));
        ParallelFor(0, rows, num_threads, [&](size_t row, size_t threadId){
            // normalize vector:
            size_t start_idx = threadId * (index->dim);
            normalize_vector((index->dim), vectors[row].data(), (norm_array.data() + start_idx));

            size_t id = *(labels + row);
            ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->addPoint((void*)(norm_array.data() + start_idx), id, static_cast<bool>(replace_deleted)); 
            });

    } catch (const std::exception& e) {
        std::cerr << "[hnsw] Exception caught: " << e.what() << std::endl;
        return 1; // Error code for C
    }

    return 0;
  
}

void markDeleted(HnswIndex *index, size_t label)
{
    ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->markDelete(label);
}

void unmarkDeleted(HnswIndex *index, size_t label)
{
    ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->unmarkDelete(label);
}

void resizeIndex(HnswIndex *index, size_t new_size)
{
    ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->resizeIndex(new_size);
}

size_t getMaxElements(HnswIndex *index)
{
    return ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->max_elements_;
}

size_t getCurrentCount(HnswIndex *index)
{
    return ((hnswlib::HierarchicalNSW<float> *)(index->hnsw))->cur_element_count;
}

SearchResult *searchKnn(HnswIndex *index, const float *flat_vectors, int rows, int k, int num_threads)
{
    //CustomFilterFunctor idFilter(filter);
    //CustomFilterFunctor *p_idFilter = filter ? &idFilter : nullptr;

    // avoid using threads when the number of searches is small:
    if (rows <= num_threads * 4)
    {
        num_threads = 1;
    }

    std::vector<std::vector<float>> vectors = convertTo2DVector(flat_vectors, rows, index->dim);

    SearchResult *searchResult = new SearchResult;
    if (!searchResult) {
        return nullptr; // Allocation failure
    }
    searchResult->label = new hnswlib::labeltype[rows * k];
    searchResult->dist = new float[rows * k];
    if (!searchResult->label || !searchResult->dist) {
        delete[] searchResult->label;
        delete[] searchResult->dist;
        delete searchResult;
        return nullptr; // Allocation failure
    }


    try {
        if (index->normalize == false) {
            ParallelFor(0, rows, num_threads, [&](size_t row, size_t threadId) {
                std::priority_queue<std::pair<float, hnswlib::labeltype>> result =
                    ((hnswlib::HierarchicalNSW<float> *)index->hnsw)->searchKnn(vectors[row].data(), k, nullptr);

                if (result.size() != (size_t)k)
                    throw std::runtime_error("Cannot return the results in a contiguous 2D array. Probably ef or M is too small");

                for (int i = k - 1; i >= 0; i--) {
                    auto& result_tuple = result.top();
                    *(searchResult->dist + row * k + i) = result_tuple.first;
                    *(searchResult->label + row * k + i) = result_tuple.second;
                    result.pop();
                }
            });

        } else {
            std::vector<float> norm_array(num_threads * (index->dim));
            ParallelFor(0, rows, num_threads, [&](size_t row, size_t threadId) {
                size_t start_idx = threadId * (index->dim);
                normalize_vector((index->dim), vectors[row].data(), (norm_array.data() + start_idx));

                std::priority_queue<std::pair<float, hnswlib::labeltype>> result =
                    ((hnswlib::HierarchicalNSW<float> *)index->hnsw)->searchKnn((void*)(norm_array.data() + start_idx), k, nullptr);

                if (result.size() != (size_t)k)
                    throw std::runtime_error("Cannot return the results in a contiguous 2D array. Probably ef or M is too small");

                for (int i = k - 1; i >= 0; i--) {
                    auto& result_tuple = result.top();
                    *(searchResult->dist + row * k + i) = result_tuple.first;
                    *(searchResult->label + row * k + i) = result_tuple.second;
                    result.pop();
                }
            });
        }
    } catch (const std::exception& e) {
        std::cerr << "[hnsw] searchKnn exception: " << e.what() << std::endl;
        delete[] searchResult->label;
        delete[] searchResult->dist;
        delete searchResult;
        return nullptr;
    }

    return searchResult;
}

int getAllowReplaceDeleted(HnswIndex *index) {
   return ((hnswlib::HierarchicalNSW<float> *)index->hnsw)->allow_replace_deleted_;
}

void getDataByLabel(HnswIndex *index, const size_t label, float* data) {
    try {
        auto vec = ((hnswlib::HierarchicalNSW<float> *)index->hnsw)->getDataByLabel<float>(label);
        // Copy data to output buffer
        for (size_t i = 0; i < vec.size(); i++) {
            data[i] = vec[i];
        }
    } catch (const std::exception& e) {
        // Label not found or other error - return zeros
        size_t dim = index->dim;
        for (size_t i = 0; i < dim; i++) {
            data[i] = 0.0f;
        }
    }
}

void freeHNSW(HnswIndex *index)
{
    hnswlib::HierarchicalNSW<float> *ptr = (hnswlib::HierarchicalNSW<float> *)index->hnsw;
    delete ptr;

    if (index->space_type == l2)
    {
        hnswlib::L2Space *space = (hnswlib::L2Space *)(index->space);
        delete space;
    }
    else if (index->space_type == ip || index->space_type == cosine)
    {
        hnswlib::InnerProductSpace *space = (hnswlib::InnerProductSpace *)(index->space);
        delete space;
    }
    else
    {
        throw std::runtime_error("Space name must be one of l2, ip, or cosine.");
    }

    delete index;
}

void freeResult(SearchResult *result)
{
    delete[] result->label;
    delete[] result->dist;
    delete result;
}

static std::vector<std::vector<float>> convertTo2DVector(const float* flat_vectors, int rows, int cols) {
    std::vector<std::vector<float>> vectors(rows, std::vector<float>(cols));
    for (int i = 0; i < rows; ++i) {
        for (int j = 0; j < cols; ++j) {
            vectors[i][j] = flat_vectors[i * cols + j];
        }
    }
    return vectors;
}