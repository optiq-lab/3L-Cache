//
// Created by Juncheng on 6/1/21.
//

#include "include/mymath.h"

#include <inttypes.h>

#include "../include/libCacheSim/logging.h"

__thread uint64_t rand_seed = 0;
__thread __uint128_t g_lehmer64_state = 0;


void set_rand_seed(uint64_t seed) { rand_seed = seed; g_lehmer64_state = seed; }
