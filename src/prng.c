#include "prng.h"

#include <stdint.h>

#include "assert.h"

#define PRNG_MULTIPLIER (747796405U)
#define PRNG_INCREMENT (1729U)

static uint32_t prngAdvance(unsigned *prng)
{
    uint32_t state;
    uint32_t n;
    state = *prng;
    n = ((state >> ((state >> 28) + 4)) ^ state) * (277803737U);
    n ^= n >> 22;
    *prng = state * PRNG_MULTIPLIER + PRNG_INCREMENT;
    return n;
}

static uint32_t prngAtMost(unsigned *prng, uint32_t salt, uint32_t max)
{
    /* We want (UINT32_MAX + 1) % max, which in unsigned arithmetic is the same
     * as (UINT32_MAX + 1 - max) % max = -max % max. We compute -max using not
     * to avoid compiler warnings.
     */
    const uint32_t min = (~max + 1U) % max;
    uint32_t x;

    if (max == (~((uint32_t)0U))) {
        return prngAdvance(prng) ^ salt;
    }

    max++;

    do {
        x = prngAdvance(prng) ^ salt;
    } while (x < min);

    return x % max;
}

int prngRange(unsigned *prng, int min, int max)
{
    uint64_t range = (uint64_t)max - (uint64_t)min;

    assert(min <= max);

    if (range > (~((uint32_t)0U))) {
        range = (~((uint32_t)0U));
    }

    return min + (int)prngAtMost(prng, 0, (uint32_t)range);
}
