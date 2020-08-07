/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

#include "../include/raft.h"

/* Emit a debug message with the given tracer. */
#define Tracef(TRACER, ...)                             \
    if (TRACER != NULL) {                               \
        char _msg[1024];                                \
        snprintf(_msg, sizeof _msg, __VA_ARGS__);       \
        TRACER->emit(TRACER, __FILE__, __LINE__, _msg); \
    }

#endif /* TRACING_H_ */
