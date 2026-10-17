// Prints the default malloc zone's size classes: each request range and the malloc_size it gets.
#include <malloc/malloc.h>
#include <stdio.h>
#include <stdlib.h>

static size_t msize(size_t n) {
    void *p = malloc(n);
    size_t s = malloc_size(p);
    free(p);
    return s;
}

int main(void) {
    printf("default zone: %s\n", malloc_get_zone_name(malloc_default_zone()));
    size_t from = 1, prev = msize(1);
    for (size_t n = 2; n <= 70000; n++) {
        size_t s = msize(n);
        if (s != prev) {
            printf("%zu..%zu -> %zu\n", from, n - 1, prev);
            from = n;
            prev = s;
        }
    }
    printf("larger requests:");
    size_t big[] = {100000, 131072, 262144, 524288, 1000000, 1044480, 1048576, 1212416, 1835008, 4194304};
    for (size_t i = 0; i < sizeof big / sizeof big[0]; i++) {
        printf(" %zu->%zu", big[i], msize(big[i]));
    }
    printf("\n");
    return 0;
}
