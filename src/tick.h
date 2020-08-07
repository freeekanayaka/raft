/* Logic to be invoked periodically. */

#ifndef TICK_H_
#define TICK_H_

#include "../include/raft.h"

/* Apply time-dependent rules for followers (Figure 3.1). */
int tickFollower(struct raft *r);

/* Apply time-dependent rules for candidates (Figure 3.1). */
int tickCandidate(struct raft *r);

/* Apply time-dependent rules for leaders (Figure 3.1). */
int tickLeader(struct raft *r);

#endif /* TICK_H_ */
