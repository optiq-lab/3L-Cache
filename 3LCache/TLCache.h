#ifndef WEBCACHESIM_TLCache_H
#define WEBCACHESIM_TLCache_H

#include "cache.h"
#include <unordered_map>
#include <unordered_set>
#include "../libCacheSim/dataStructure/sparsepp/spp.h"
#include <vector>
#include <random>
#include <cmath>
#include <LightGBM/c_api.h>
#include <assert.h>
#include <sstream>
#include <fstream>
#include <list>
#include <deque>
using namespace webcachesim;
using namespace std;

using spp::sparse_hash_map;

namespace TLCache {
    static const uint8_t max_n_past_timestamps = 4;
    // Number of  inter-arrival times.
    static const uint8_t max_n_past_distances = 3;
    // Number of training samples.
    static const uint32_t batch_size = 131072 / 2;
// heap metadata
struct HeapUint {
    float reuse_time;
    uint64_t key;
};

// Metadata of object information
struct MetaExtra {
    vector<uint32_t> _past_distances;
    //the next index to put the distance
    uint8_t _past_distance_idx = 1;

    MetaExtra(const uint32_t &distance) {
        _past_distances = vector<uint32_t>(1, distance);
    }

    void update(const uint32_t &distance) {
        uint8_t distance_idx = _past_distance_idx % max_n_past_distances;
        if (_past_distances.size() < max_n_past_distances)
            _past_distances.emplace_back(distance);
        else
            _past_distances[distance_idx] = distance;
        assert(_past_distances.size() <= max_n_past_distances);
        _past_distance_idx = _past_distance_idx + (uint8_t) 1;
        if (_past_distance_idx >= max_n_past_distances * 2)
            _past_distance_idx -= max_n_past_distances;
    }
};

class Meta {
public:
    uint64_t _key;
    uint32_t _size;
    uint64_t _past_timestamp;
    uint16_t _freq;
    MetaExtra *_extra = nullptr;

    uint64_t _sample_times;

    Meta(const uint64_t &key, const uint64_t &size, const uint64_t &past_timestamp) {
        _key = key;
        _size = size;
        _past_timestamp = past_timestamp;
        _freq = 1;
        _sample_times = 0;
    }

    virtual ~Meta() = default;

    void emplace_sample(uint64_t &sample_t, uint8_t max_num = 1) {
        if (_sample_times == 0)
            _sample_times = sample_t;
    }

    void free() {
        delete _extra;
    }
    void update(const uint64_t &past_timestamp) {
        if (max_n_past_distances > 0) {
            uint32_t _distance = past_timestamp - _past_timestamp;
            assert(_distance);
            if (!_extra) {
                _extra = new MetaExtra(_distance);
            } else
                _extra->update(_distance);
        }
        _past_timestamp = past_timestamp;
        if (_freq < 65535)
            _freq++;
    }
};



struct CircleList {
    uint32_t prev = 0;
    uint32_t next = 0;
};

struct LinkHE {
    uint32_t head = -1;
    uint32_t tail = -1;
};

// The linked list structure of cache queue.
class CacheUpdateQueue {
public:
    deque<Meta> metas;
    // The position of the first element in the queue.
    uint32_t front_index=0;
    vector<CircleList> dq;
    LinkHE q;

    // hit, move a object to tail/head
    uint32_t re_request(const uint32_t pos) {
        if (pos == q.head){
            q.tail = q.head;
            q.head = dq[q.head].next;
        } else if (pos != q.tail) {
            uint32_t next = dq[pos].next;
            uint32_t prev = dq[pos].prev;
            dq[prev].next = next;
            dq[next].prev = prev;

            dq[pos].next = q.head;
            dq[q.head].prev = pos;
            dq[pos].prev = q.tail;
            dq[q.tail].next = pos;
            q.tail = pos;
        }
        return q.tail;
    }

    // new object request, insert a new object into tail/head
    uint32_t request(const uint32_t pos) {
        if (q.head == -1) {
            q.head = pos;
            q.tail = pos;
            dq[pos].next = pos;
            dq[pos].prev = pos;
        } else {
            dq[q.tail].next = pos;
            dq[q.head].prev = pos;
            dq[pos].prev = q.tail;
            dq[pos].next = q.head;
            q.tail = pos;
        }
        
        return q.tail;
    }

    // remove objects from the cache queue during eviction.
    void erase(const uint32_t pos) {
        uint32_t next = dq[pos].next;
        uint32_t prev = dq[pos].prev;
        if(pos == q.head) {
            q.head = next;
        }else if(pos == q.tail){
            q.tail = prev;
        }
        dq[prev].next = next;
        dq[next].prev = prev;
    }
};


class TrainingData {
public:
    vector<float> labels;
    vector<int32_t> indptr;
    vector<int32_t> indices;
    vector<double> data;
    TrainingData(uint32_t n_feature) {
        labels.reserve(batch_size);
        indptr.reserve(batch_size + 1);
        indptr.emplace_back(0);
        indices.reserve(batch_size * n_feature);
        data.reserve(batch_size * n_feature);
    }

    void emplace_back(Meta &meta, uint64_t &sample_timestamp, uint32_t &future_interval, const uint64_t &key) {
        int32_t counter = indptr.back();

        indices.emplace_back(0);
        // waitting time.
        data.emplace_back(sample_timestamp - meta._past_timestamp);
        ++counter;
        int j = 0;
        // freqency.
        uint16_t n_within = meta._freq;
        // inter-arrival times.
        if (meta._extra) {
            for (; j < meta._extra->_past_distance_idx && j < max_n_past_distances; ++j) {
                uint8_t past_distance_idx = (meta._extra->_past_distance_idx - 1 - j) % max_n_past_distances;
                const uint32_t &past_distance = meta._extra->_past_distances[past_distance_idx];
                indices.emplace_back(j + 1);
                data.emplace_back(past_distance);
            }
        }

        counter += j;

        indices.emplace_back(max_n_past_timestamps);
        data.push_back(meta._size);
        ++counter;


        indices.push_back(max_n_past_timestamps + 1);
        data.push_back(n_within);
        ++counter;
        labels.push_back(log1p(future_interval));
        indptr.push_back(counter);

    }

    void clear() {
        labels.clear();
        indptr.resize(1);
        indices.clear();
        data.clear();
    }
};

struct KeyMapEntryT {
    uint8_t list_idx;
    uint32_t list_pos;
};

class TLCacheCache : public Cache {
public:
    uint64_t current_seq = -1;
    uint32_t n_feature;
    sparse_hash_map<uint64_t, float> pred_map;
    // Used to record the predicted results of objects and also record IDs.
    vector<HeapUint> pred_times;
    // scan length
    uint64_t scan_length = 0;
    // new objct
    vector<uint64_t> new_obj_keys;
    uint64_t new_obj_size = 0;
    // eviction count
    int evict_nums = 0;
    // Sample rate L, adjustable
    uint16_t sample_rate = 1024;
    // prediction eviction ratio
    uint8_t eviction_rate = 2;
    // f
    uint16_t sample_boundary = 1;
    // x
    uint8_t sampling_lru = 1;
    uint64_t *evcition_distribution = (uint64_t*)malloc(sizeof(uint64_t) * 4);
    uint32_t *object_distribution_n_eviction = (uint32_t*)malloc(sizeof(uint32_t) * 16);
    uint32_t initial_queue_length = 0;
    uint64_t origin_current_seq = 0;
    // Q
    uint8_t reserved_space = 2;
    // sampling pointer
    uint32_t samplepointer = 0;
    uint8_t hsw = 2;
    uint64_t MAX_EVICTION_BOUNDARY[2] = {0, 0};
    uint32_t max_out_cache_size = 2;
    uint8_t is_full = 0;
    // miss ratio
    uint64_t n_req = 0;
    uint64_t n_hit = 0;
    uint64_t n_window_hit=0;
    uint64_t spointer_timestamp = 0;
    // Slide object information within the window
    sparse_hash_map<uint64_t, KeyMapEntryT> key_map;

    // cache queue
    CacheUpdateQueue in_cache;
    // history information
    CacheUpdateQueue out_cache;

    TrainingData *training_data;

    double training_loss = 0;
    int32_t n_force_eviction = 0;

    double training_time = 0;
    double inference_time = 0;

    // Determine whether the model has been trained.
    BoosterHandle booster = nullptr;

    // Model training parameters
    unordered_map<string, string> training_params = {
            {"boosting",         "gbdt"},
            {"objective",        "regression"},
            {"num_iterations",   "16"},
            {"num_leaves",       "32"},
            {"num_threads",      "1"},
            {"feature_fraction", "0.8"},
            {"bagging_freq",     "5"},
            {"bagging_fraction", "0.8"},
            {"learning_rate",    "0.1"},
            {"verbosity",        "-1"},
    };

    unordered_map<string, string> inference_params;

    enum ObjectiveT : uint8_t {
        byte_miss_ratio = 0, object_miss_ratio = 1
    };
    ObjectiveT objective = byte_miss_ratio;

    // random seed
    default_random_engine _generator = default_random_engine();
    uniform_int_distribution<std::size_t> _distribution = uniform_int_distribution<std::size_t>();
    bool is_sampling = false;

    uint64_t byte_million_req;
    void init_with_params(const map<string, string> &params) override {
        //set params
        for (auto &it: params) {
            if (it.first == "num_iterations") {
                training_params["num_iterations"] = it.second;
            } else if (it.first == "learning_rate") {
                training_params["learning_rate"] = it.second;
            } else if (it.first == "num_threads") {
                training_params["num_threads"] = it.second;
            } else if (it.first == "num_leaves") {
                training_params["num_leaves"] = it.second;
            } else if (it.first == "byte_million_req") {
                byte_million_req = stoull(it.second);
            } else if(it.first == "sample_rate") {
                sample_rate = stoull(it.second);
            } else if (it.first == "objective") {
                if (it.second == "byte-miss-ratio")
                    objective = byte_miss_ratio;
                else if (it.second == "object-miss-ratio")
                    objective = object_miss_ratio;
                else {
                    cerr << "error: unknown objective" << endl;
                    exit(-1);
                }
            } else {
                cerr << "3LCache unrecognized parameter: " << it.first << endl;
            }
        }

        if (objective == object_miss_ratio)
            sample_boundary = -1;
        memset(object_distribution_n_eviction, 0, sizeof(uint32_t) * 16);
        memset(evcition_distribution, 0, sizeof(uint64_t) * 4);
        n_feature = max_n_past_timestamps + 2;
        inference_params = training_params;
        training_data = new TrainingData(n_feature);
    }

    bool lookup(const SimpleRequest &req) override;

    void admit(const SimpleRequest &req) override;

    void evict();

    void forget();

    void erase_out_cache();

    uint32_t rank();

    void evict_with_candidate(pair<uint64_t, uint32_t> &epair);

    vector<uint32_t>  quick_demotion();

    void train();

    void prediction(vector<uint32_t> sampled_objects);

    void sample();

    void update_stat_periodic() override;

    pair<uint64_t, uint32_t> evict_predobj();

    void remove_from_outcache_metas(Meta &meta, unsigned int &pos, const uint64_t &key);

    vector<int> get_object_distribution_n_past_timestamps() {
        vector<int> distribution(max_n_past_timestamps, 0);
        for (auto &meta: in_cache.metas) {
            if (nullptr == meta._extra) {
                ++distribution[0];
            } else {
                ++distribution[meta._extra->_past_distances.size()];
            }
        }
        for (auto &meta: out_cache.metas) {
            if (nullptr == meta._extra) {
                ++distribution[0];
            } else {
                ++distribution[meta._extra->_past_distances.size()];
            }
        }
        return distribution;
    }

};

}
#endif

