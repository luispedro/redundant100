#include <stdint.h>
extern "C" {
int rollhash(const unsigned char* seq, const int len, const int n, uint64_t* out);
void* init_hash();
 void destroy_hash(void* h);
void insert_hash(void * h, uint64_t key, const char* header, const char* s);
char* write_matches(const void* h, const int n, const char* header, const int header_n, const char* seq, const int seq_n);

}
