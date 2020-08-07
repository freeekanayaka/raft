#include "../../src/prng.h"
#include "../lib/runner.h"

SUITE(prngRange)

/* First generated number with the default seed. */
TEST(prngRange, first, NULL, NULL, 0, NULL)
{
    unsigned prng = 42;
    int n = prngRange(&prng, 1000, 2000);
    munit_assert_int(n, ==, 1650);
    return MUNIT_OK;
}

/* Sequence of 10 numbers with the default seed. */
TEST(prngRange, sequence, NULL, NULL, 0, NULL)
{
    unsigned prng = 42;
    int n[10];
    int i;
    int j;
    for (i = 0; i < 10; i++) {
        n[i] = prngRange(&prng, 1000, 2000);
    }
    for (i = 0; i < 9; i++) {
        munit_assert_int(n[i], >=, 1000);
        munit_assert_int(n[i], <=, 2000);
        for (j = i + 1; j < 10; j++) {
            munit_assert_int(n[i], !=, n[j]);
        }
    }
    return MUNIT_OK;
}

/* Change the seed */
TEST(prngRange, seed, NULL, NULL, 0, NULL)
{
    unsigned prng = 0;
    int n;
    int i;
    int expected[20] = {1571, 1410, 1735, 1743, 1995, 1353, 1589,
                        1478, 1753, 1367, 1112, 1216, 1727, 1057,
                        1061, 1669, 1773, 1425, 1864, 1035};
    for (i = 0; i < 20; i++) {
        n = prngRange(&prng, 1000, 2000);
        munit_assert_int(n, ==, expected[i]);
    }
    return MUNIT_OK;
}
