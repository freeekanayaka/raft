#ifndef BACKEND_METADATA_H_
#define BACKEND_METADATA_H_

#include "../include/raft/backend.h"

void BackendMetadataEncode(struct raft_backend *s,
                           raft_term term,
                           raft_id voted_for);

#endif /* BACKEND_METADATA_H_ */
