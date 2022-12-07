#include "backend_TrieStoreCBackend.h"
#include "TrieStore.h"
#include <string>

TrieStore trieStore;

#ifdef ROWSTORE
RowStore store;
#endif

#ifdef COLSTORE
ColumnStore store;
#endif

#if defined(COLSTORE) || defined(ROWSTORE)
JNIEXPORT jintArray JNICALL Java_backend_TrieStoreCBackend_readMultiCuboid0
        (JNIEnv *env, jobject obj, jstring jfilename, jbooleanArray jIsSparse, jintArray jNumCols, jintArray jNumRows) {
    jsize numCuboids = env->GetArrayLength(jIsSparse);
    const char *filename = env->GetStringUTFChars(jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(jIsSparse, 0);
    jint *numColsArray = env->GetIntArrayElements(jNumCols, 0);
    jint *numRowsArray = env->GetIntArrayElements(jNumRows, 0);
    unsigned int *idsArray = new unsigned int[numCuboids];
    store.readMultiCuboid(filename, numColsArray, numRowsArray, isSparseArray, idsArray, numCuboids);
    jintArray jids = env->NewIntArray(numCuboids);
    env->SetIntArrayRegion(jids, 0, numCuboids, (jint *) idsArray);
    env->ReleaseStringUTFChars(jfilename, filename);
    env->ReleaseBooleanArrayElements(jIsSparse, isSparseArray, 0);
    env->ReleaseIntArrayElements(jNumCols, numColsArray, 0);
    env->ReleaseIntArrayElements(jNumRows, numRowsArray, 0);
    delete[] idsArray;
    return jids;
}

JNIEXPORT jboolean JNICALL Java_backend_TrieStoreCBackend_saveCuboid0
        (JNIEnv *env, jobject obj, jintArray jdims, jint hid) {
    auto dims = env->GetIntArrayElements(jdims, nullptr);
    auto len = env->GetArrayLength(jdims);
    std::vector<int> dimsVec(dims, dims + len);
    jboolean ret = true;
    if (hid >= 0) { //sparse
        ret = trieStore.addSparseCuboidToTrie(store, dimsVec, hid);
    } else { //dense
        ret = trieStore.addDenseCuboidToTrie(store, dimsVec, -hid);
    }
    env->ReleaseIntArrayElements(jdims, dims, 0);
    return ret;
}

#else
JNIEXPORT jintArray JNICALL Java_backend_TrieStoreCBackend_readMultiCuboid0
        (JNIEnv *env, jobject obj, jstring jfilename, jbooleanArray jIsSparse, jintArray jNumCols, jintArray jNumRows) {
    throw std::runtime_error("Trie store cannot load cuboids. Recompile with ROWSTORE or COLSTORE");
}
JNIEXPORT jboolean JNICALL Java_backend_TrieStoreCBackend_saveCuboid0
        (JNIEnv *env, jobject obj, jintArray jdims, jint hid) {
     throw std::runtime_error("Trie store cannot cuboids. Recompile with ROWSTORE or COLSTORE");
}
#endif

JNIEXPORT void JNICALL Java_backend_TrieStoreCBackend_setPrimaryMoments0
        (JNIEnv *env, jobject obj, jdoubleArray jpm) {
    auto len = env->GetArrayLength(jpm);
    auto pm = env->GetDoubleArrayElements(jpm, nullptr);
    trieStore.setPrimaryMoments(pm, len);

}


JNIEXPORT void JNICALL Java_backend_TrieStoreCBackend_saveTrie0
        (JNIEnv *env, jobject obj, jstring jfilename) {
    const char *filename = env->GetStringUTFChars(jfilename, nullptr);
    trieStore.saveTrie(filename);
    env->ReleaseStringUTFChars(jfilename, filename);
}


JNIEXPORT void JNICALL Java_backend_TrieStoreCBackend_initTrie0
        (JNIEnv *env, jobject obj, jlong ms) {
    trieStore.globalSetTrie.init(ms);
}


JNIEXPORT void JNICALL Java_backend_TrieStoreCBackend_loadTrie0
        (JNIEnv *env, jobject obj, jstring jfilename) {
    const char *filename = env->GetStringUTFChars(jfilename, nullptr);
    trieStore.loadTrie(filename);
    env->ReleaseStringUTFChars(jfilename, filename);
}


JNIEXPORT jdoubleArray JNICALL Java_backend_TrieStoreCBackend_prepareFromTrie0
        (JNIEnv *env, jobject obj, jintArray jquery, jintArray jslice) {
    auto query = env->GetIntArrayElements(jquery, nullptr);
    auto slice = env->GetIntArrayElements(jslice, nullptr);
    auto totalquerylen = env->GetArrayLength(jquery);
    int agglen = 0;
    std::vector<int> sliceValues(slice, slice + totalquerylen);
    for (auto v: sliceValues)
        agglen += (v == -1);
    std::vector<int> queryVec(query, query + totalquerylen);

    std::vector<double> resultVec((1 << agglen));
    trieStore.prepareFromTrie(queryVec, resultVec, sliceValues);
    env->ReleaseIntArrayElements(jquery, query, 0);
    env->ReleaseIntArrayElements(jslice, slice, 0);
    auto jresult = env->NewDoubleArray(1u << agglen);
    env->SetDoubleArrayRegion(jresult, 0, 1 << agglen, &resultVec[0]);
    return jresult;
}

