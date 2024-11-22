#define _GNU_SOURCE

#include <fcntl.h>
#include <stdio.h>

int main() {
    printf("%d\n", O_DIRECT);
    return 0;
}