#include "backend_metadata.h"
#include "assert.h"
#include "byte.h"

#define BACKEND_METADATA_PREFIX "metadata"

/* Render the filename of the metadata file with index @n. */
static void backendMetadataFilename(const unsigned short n, char *filename)
{
    sprintf(filename, BACKEND_METADATA_PREFIX "%d", n);
}

/* Return the metadata file index associated with the given version. */
static unsigned short backendMetadataFileIndex(unsigned long long version)
{
    return version % 2 == 1 ? 1 : 2;
}

void BackendMetadataEncode(struct raft_backend *s,
                           raft_term term,
                           raft_id voted_for)
{
    void *cursor;
    unsigned short n;

    /* Bump the metadata version */
    s->metadata.version++;

    /* Encode the file content. */
    cursor = &s->metadata.content;
    bytePut64(&cursor, RAFT_BACKEND_FORMAT_V1);
    bytePut64(&cursor, s->metadata.version);
    bytePut64(&cursor, term);
    bytePut64(&cursor, voted_for);

    /* Render the metadata file name. */
    n = backendMetadataFileIndex(s->metadata.version);
    backendMetadataFilename(n, s->metadata.filename);
}
