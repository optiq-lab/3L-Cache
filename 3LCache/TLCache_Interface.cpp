

#include <assert.h>

#include <map>
#include <string>

#include "../libCacheSim/libCacheSim/dataStructure/hashtable/hashtable.h"
#include "../libCacheSim/libCacheSim/include/libCacheSim/cache.h"
#include "TLCache.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  void *TLCache_cache;
  char *objective;
  SimpleRequest TLCache_req;

  pair<uint64_t, uint32_t> to_evict_pair;
  cache_obj_t obj_tmp;
} TLCache_params_t;

static const char *DEFAULT_PARAMS = "objective=byte-miss-ratio";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void TLCache_free(cache_t *cache);
static bool TLCache_get(cache_t *cache, const request_t *req);

static cache_obj_t *TLCache_find(cache_t *cache, const request_t *req,
                             const bool update_cache);
static cache_obj_t *TLCache_insert(cache_t *cache, const request_t *req);
static cache_obj_t *TLCache_to_evict(cache_t *cache, const request_t *req);
static void TLCache_evict(cache_t *cache, const request_t *req);
static bool TLCache_remove(cache_t *cache, const obj_id_t obj_id);
static int64_t TLCache_get_occupied_byte(const cache_t *cache);
static int64_t TLCache_get_n_obj(const cache_t *cache);

static void TLCache_parse_params(cache_t *cache, const char *cache_specific_params);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************

/**
 * @brief initialize the cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params cache specific parameters, see parse_params
 * function or use -e "print" with the cachesim binary
 */
cache_t *TLCache_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params) {
#ifdef SUPPORT_TTL
  if (ccache_params.default_ttl < 30 * 86400) {
    ERROR("TLCache does not support expiration\n");
    abort();
  }
#endif

  cache_t *cache = cache_struct_init("TLCache", ccache_params, cache_specific_params);
  cache->cache_init = TLCache_init;
  cache->cache_free = TLCache_free;
  cache->get = TLCache_get;
  cache->find = TLCache_find;
  cache->insert = TLCache_insert;
  cache->evict = TLCache_evict;
  cache->to_evict = TLCache_to_evict;
  cache->remove = TLCache_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_occupied_byte = TLCache_get_occupied_byte;
  cache->get_n_obj = TLCache_get_n_obj;
  cache->to_evict_candidate =
      static_cast<cache_obj_t *>(malloc(sizeof(cache_obj_t)));

  if (ccache_params.consider_obj_metadata) {
    cache->obj_md_size = 180;
  } else {
    cache->obj_md_size = 0;
  }

  auto *params = my_malloc(TLCache_params_t);
  memset(params, 0, sizeof(TLCache_params_t));
  cache->eviction_params = params;

  if (cache_specific_params != NULL) {
    TLCache_parse_params(cache, cache_specific_params);
  } else {
    TLCache_parse_params(cache, DEFAULT_PARAMS);
  }

  auto *TLCache = new TLCache::TLCacheCache();
  params->TLCache_cache = static_cast<void *>(TLCache);

  TLCache->setSize(ccache_params.cache_size);

  std::map<string, string> params_map;

  params_map["objective"] = params->objective;

  if (strcmp(params->objective, "object-miss-ratio") == 0) {
    snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "%s", "TLCache-OMR");
  } else if (strcasecmp(params->objective, "byte-miss-ratio") == 0) {
    snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "%s", "TLCache-BMR");
  } else {
    ERROR("TLCache does not support objective %s\n", params->objective);
  }

  TLCache->init_with_params(params_map);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void TLCache_free(cache_t *cache) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);
  delete TLCache;
  free(cache->to_evict_candidate);
  my_free(sizeof(TLCache_params_t), params);
  cache_struct_free(cache);
}

/**
 * @brief this function is the user facing API
 * it performs the following logic
 *
 * ```
 * if obj in cache:
 *    update_metadata
 *    return true
 * else:
 *    if cache does not have enough space:
 *        evict until it has space to insert
 *    insert the object
 *    return false
 * ```
 *
 * @param cache
 * @param req
 * @return true if cache hit, false if cache miss
 */
static bool TLCache_get(cache_t *cache, const request_t *req) {
  return cache_get_base(cache, req);
}

// ***********************************************************************
// ****                                                               ****
// ****       developer facing APIs (used by cache developer)         ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief find an object in the cache
 *
 * @param cache
 * @param req
 * @param update_cache whether to update the cache,
 *  if true, the object is promoted
 *  and if the object is expired, it is removed from the cache
 * @return the object or NULL if not found
 */
static cache_obj_t *TLCache_find(cache_t *cache, const request_t *req,
                             const bool update_cache) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);

  if (!update_cache) {
    bool is_hit = TLCache->exist(static_cast<int64_t>(req->obj_id));
    return is_hit ? reinterpret_cast<cache_obj_t *>(0x1) : NULL;
  }

  params->TLCache_req.reinit(cache->n_req, req->obj_id, req->obj_size, nullptr);
  bool is_hit = TLCache->lookup(params->TLCache_req);

  if (is_hit) {
    return reinterpret_cast<cache_obj_t *>(0x1);
  } else {
    return NULL;
  }
}

/**
 * @brief insert an object into the cache,
 * update the hash table and cache metadata
 * this function assumes the cache has enough space
 * eviction should be
 * performed before calling this function
 *
 * @param cache
 * @param req
 * @return the inserted object
 */
static cache_obj_t *TLCache_insert(cache_t *cache, const request_t *req) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);
  params->TLCache_req.reinit(cache->n_req, req->obj_id, req->obj_size, nullptr);

  TLCache->admit(params->TLCache_req);

  return reinterpret_cast<cache_obj_t *>(0x1);
}

/**
 * @brief find an eviction candidate, but do not evict from the cache,
 * and do not update the cache metadata
 * note that eviction must evicts this object, so if we implment this function
 * and it uses random number, we must make sure that the same object is evicted
 * when we call evict
 *
 * @param cache
 * @param req
 * @return cache_obj_t*
 */
static cache_obj_t *TLCache_to_evict(cache_t *cache, const request_t *req) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);
  // TLCache rank变成了evict_preobj
  params->to_evict_pair = TLCache->evict_predobj();
  auto &meta = TLCache->in_cache.metas[params->to_evict_pair.second];

  params->obj_tmp.obj_id = params->to_evict_pair.first;
  params->obj_tmp.obj_size = meta._size;

  cache->to_evict_candidate = &params->obj_tmp;
  cache->to_evict_candidate_gen_vtime = cache->n_req;

  return cache->to_evict_candidate;
}

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 * @param evicted_obj if not NULL, return the evicted object to caller
 */
static void TLCache_evict(cache_t *cache, const request_t *req) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);

  if (cache->to_evict_candidate_gen_vtime == cache->n_req) {
    TLCache->evict_with_candidate(params->to_evict_pair);
    cache->to_evict_candidate_gen_vtime = -1;
  } else {
    TLCache->evict();
  }
}

/**
 * @brief remove an object from the cache
 * this is different from cache_evict because it is used to for user trigger
 * remove, and eviction is used by the cache to make space for new objects
 *
 * it needs to call cache_remove_obj_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param obj_id
 * @return true if the object is removed, false if the object is not in the
 * cache
 */
static bool TLCache_remove(cache_t *cache, const obj_id_t obj_id) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);

  ERROR("do not support remove");
  return true;
}

static int64_t TLCache_get_n_obj(const cache_t *cache) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);

  return TLCache->in_cache.metas.size();
}

static int64_t TLCache_get_occupied_byte(const cache_t *cache) {
  auto *params = static_cast<TLCache_params_t *>(cache->eviction_params);
  auto *TLCache = static_cast<TLCache::TLCacheCache *>(params->TLCache_cache);

  return TLCache->_currentSize;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *TLCache_current_params(cache_t *cache, TLCache_params_t *params) {
  static __thread char params_str[128];
  int n = snprintf(params_str, 128, "objective=%s", params->objective);

  snprintf(cache->cache_name + n, 128 - n, "\n");

  return params_str;
}

static void TLCache_parse_params(cache_t *cache,
                             const char *cache_specific_params) {
  TLCache_params_t *params = (TLCache_params_t *)cache->eviction_params;
  char *params_str = strdup(cache_specific_params);
  char *end;

  while (params_str != NULL && params_str[0] != '\0') {
    /* different parameters are separated by comma,
     * key and value are separated by = */
    char *key = strsep((char **)&params_str, "=");
    char *value = strsep((char **)&params_str, ",");

    // skip the white space
    while (params_str != NULL && *params_str == ' ') {
      params_str++;
    }

    if (strcasecmp(key, "objective") == 0) {
      params->objective = strdup(value);
      if (params->objective == NULL) {
        ERROR("out of memory %s\n", strerror(errno));
      }
    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", TLCache_current_params(cache, params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s\n", cache->cache_name, key);
      exit(1);
    }
  }
  free(params_str);
}
#ifdef __cplusplus
}
#endif
