#include<stdio.h>
#include<time.h>
#include <string.h>
#include <chrono>

#include "Payload.h"
extern void readMultiCuboid(const char *filename,  int n_bits_array[], int size_array[], unsigned char isSparse_array[], unsigned int id_array[], unsigned int numCuboids);

extern unsigned int shybridhash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
extern unsigned int srehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
extern unsigned int d2srehash(unsigned int d_id, unsigned int *maskpos, unsigned int masksum);
extern unsigned int drehash(unsigned int d_id, unsigned int *maskpos, unsigned int masksum);

extern unsigned int s2drehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);

unsigned int s2drehash_optimized1(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized2(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized3(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized4(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized5(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized6(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized7(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized8(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized9(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized10(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized11(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
unsigned int s2drehash_optimized12(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);

extern void s2drehash_assert(unsigned int s_id, unsigned int *maskpos, unsigned int masksum, unsigned int optimization);

double profile(unsigned int s_id, unsigned int *maskpos, unsigned int masksum, unsigned int(*s2drehash)(unsigned int, unsigned int*, unsigned int)) {
    auto start = std::chrono::steady_clock::now();

    s2drehash(s_id, maskpos, masksum);

    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end-start;
    return elapsed_seconds.count();
}

void compare_time(unsigned int s_id, unsigned int *maskpos, unsigned int masksum, unsigned int(*s2drehash_optimized)(unsigned int, unsigned int*, unsigned int masksum)) {
    double total_exec_time = 0;
    double total_exec_time_optimized = 0;
    int runs = 10;

    for (int i=0; i<runs; i++) {
        double exec_time = profile(s_id, maskpos, masksum, &s2drehash);
        total_exec_time += exec_time;

        double exec_time_optimized = profile(s_id, maskpos, masksum, s2drehash_optimized); // CHANGE HERE
        total_exec_time_optimized += exec_time_optimized;
    }

    printf("average elapsed time: %fs\n", total_exec_time/runs);
    printf("average elapsed time optimized: %fs\n", total_exec_time_optimized/runs);
}

void print_mask(unsigned int *maskpos, unsigned int masksum){
    printf("mask: {");
    for (int i=0; i<masksum; i++){
        printf("%d,",maskpos[i]);
    } printf("}\n");
}

int main(int argc, char* argv[]) {
    const char filename[] = "cubedata/NYC_base/multicube_0.csuk";
    int n_bits_array[] = {429};
    int size_array[] = {92979827};
    unsigned char isSparse_array[] = {1};
    unsigned int id_array[] = {0};
    unsigned int numCuboids = 1;

    readMultiCuboid(filename,  n_bits_array, size_array, isSparse_array, id_array, numCuboids);

    // get instruction counts
    unsigned int maskID = atoi(argv[1]);
    if (maskID == 1) {
        unsigned int maskpos[] = {0,1,2,3}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized1(id_array[0], maskpos, masksum); // CHANGE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized1); // CHANGE
    } else if (maskID == 2) {
        unsigned int maskpos[] = {0,1,2,3,4,5,6,7}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized2(id_array[0], maskpos, masksum); // CHANGE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized2); // CHANGE
    } else if (maskID == 3) {
        unsigned int maskpos[] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized3(id_array[0], maskpos, masksum); // CHANGE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized3); // CHANGE
    } else if (maskID == 4) {
        unsigned int maskpos[] = {4,5,6,7}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized4(id_array[0], maskpos, masksum); // CHANGE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized4); // CHANGE
    } else if (maskID == 5) {
        unsigned int maskpos[] = {4,5,6,7,12,13,14,15}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized5(id_array[0], maskpos, masksum); // CHANGE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized5); // CHANGE
    } else if (maskID == 6) {
        unsigned int maskpos[] = {1,3,5,7}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized6(id_array[0], maskpos, masksum); // CHANGE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized6); // CHANGE
    } else if (maskID == 7) {
        unsigned int maskpos[] = {1,3,5,7,9,11,13,15}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized7(id_array[0], maskpos, masksum); // CHANGE HERE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized7); // CHANGE
    } else if (maskID == 8) {
        unsigned int maskpos[] = {1,34,203,301}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized8(id_array[0], maskpos, masksum); // CHANGE HERE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized8); // CHANGE
    } else if (maskID == 9) {
        unsigned int maskpos[] = {1,34,73,145,203,262,301,390}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized9(id_array[0], maskpos, masksum); // CHANGE HERE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized9); // CHANGE
    } else if (maskID == 10) {
        unsigned int maskpos[] = {1,34,73,88,120,145,203,210,237,262,285,291,301,333,345,390}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized10(id_array[0], maskpos, masksum); // CHANGE HERE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized10); // CHANGE
    } else if (maskID == 11) {
        unsigned int maskpos[] = {1,34,73,88,145,203,210,262,285,291,333,390}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized11(id_array[0], maskpos, masksum); // CHANGE HERE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized11); // CHANGE
    } else if (maskID == 12) {
        unsigned int maskpos[] = {1,34,48,73,88,103,120,145,180,203,210,237,254,262,285,291,301,315,333,345,390,405}; // CHANGE HERE
        unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);
        print_mask(maskpos, masksum);
        if(!strcmp(argv[2], "-standard")) s2drehash(id_array[0], maskpos, masksum);
        else if(!strcmp(argv[2], "-optimized")) s2drehash_optimized12(id_array[0], maskpos, masksum); // CHANGE HERE
        else if(!strcmp(argv[2], "-profile")) compare_time(id_array[0], maskpos, masksum, &s2drehash_optimized12); // CHANGE
    }

    // unsigned int maskpos[] = {1,3,5,7,9,11,13,15}; // CHANGE HERE
    // unsigned int maskID = 7; // CHANGE HERE
    // unsigned int masksum = sizeof(maskpos)/sizeof(maskpos[0]);

    // printf("mask: {");
    // for (int i=0; i<masksum; i++){
    //     printf("%d,",maskpos[i]);
    // } printf("}\n");


    // s2drehash(id_array[0], maskpos, masksum);
    // // s2drehash_optimized7(id_array[0], maskpos, masksum);

    // s2drehash_assert(id_array[0], maskpos, masksum, maskID);

    // start profiling here
    // double total_exec_time = 0;
    // double total_exec_time_optimized = 0;
    // int runs = 10;

    // for (int i=0; i<runs; i++) {
    //     double exec_time = profile(id_array[0], maskpos, masksum, &s2drehash);
    //     total_exec_time += exec_time;

    //     double exec_time_optimized = profile(id_array[0], maskpos, masksum, &s2drehash_optimized7); // CHANGE HERE
    //     total_exec_time_optimized += exec_time_optimized;
    // }

    // printf("average elapsed time: %fs\n", total_exec_time/runs);
    // printf("average elapsed time optimized: %fs\n", total_exec_time_optimized/runs);
    // // end profiling here



    return 0;
}


