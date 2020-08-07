/* Set up the initial server state. */

#ifndef START_H_
#define START_H_

#include "../include/raft.h"

int startRestore(struct raft *r,
		 raft_term term,
		 raft_id voted_for,
                 struct raft_snapshot_metadata *snapshot_metadata,
                 raft_index start_index,
                 struct raft_entry *entries,
                 unsigned n_entries);

int startConvert(struct raft *r);

#endif /* START_H_ */
