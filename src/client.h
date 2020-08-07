/* Handle client requests. */

#ifndef CLIENT_H_
#define CLIENT_H_

#include "../include/raft.h"

int clientAccept(struct raft *r, struct raft_buffer *commands, unsigned n);

#endif /* CLIENT_H_ */
