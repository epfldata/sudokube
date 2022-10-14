//
// Created by Sachin Basil John on 27.08.22.
//

#include "ColumnStore.h"
#include "backend_ColumnStoreCBackend.h"

ColumnStore colStore;
JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_reset0
        (JNIEnv *env, jobject obj) {
    colStore.clear();
}

JNIEXPORT jint JNICALL Java_backend_ColumnStoreCBackend_sRehash0
        (JNIEnv *env, jobject obj, jint s_id, jintArray jbitpos, jint mode) {
    jsize bitposlen = env->GetArrayLength(jbitpos);
    auto bitpos = env->GetIntArrayElements(jbitpos, 0);
    std::vector<unsigned int> bitposvec(bitpos, bitpos + bitposlen);
    auto newid = colStore.srehash(s_id, bitposvec, mode);
    env->ReleaseIntArrayElements(jbitpos, bitpos, 0);
    return newid;
}


JNIEXPORT jint JNICALL Java_backend_ColumnStoreCBackend_dRehash0
        (JNIEnv *env, jobject obj, jint d_id, jintArray jbitpos) {
    jsize bitposlen = env->GetArrayLength(jbitpos);
    auto bitpos = env->GetIntArrayElements(jbitpos, 0);
    std::vector<unsigned int> bitposvec(bitpos, bitpos + bitposlen);
    auto newid = colStore.drehash(d_id, bitposvec);
    env->ReleaseIntArrayElements(jbitpos, bitpos, 0);
    return newid;
}


JNIEXPORT jlongArray JNICALL Java_backend_ColumnStoreCBackend_sRehashSlice0
        (JNIEnv *env, jobject obj, jint s_id, jintArray jbitpos, jbooleanArray jslicemask) {
    throw std::runtime_error("ColStore::sRehashSlice0 Not yet implemented");
}


JNIEXPORT jlongArray JNICALL Java_backend_ColumnStoreCBackend_dRehashSlice0
        (JNIEnv *env, jobject obj, jint d_id, jintArray jbitpos, jbooleanArray jslicemask) {
    throw std::runtime_error("ColStore::dRehashSlice0 Not yet implemented");
}


JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_saveAsTrie0
        (JNIEnv *env, jobject obj, jobjectArray, jstring, jlong) {
    throw std::runtime_error("ColStore::saveAsTrie Not yet implemented");
}


JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_loadTrie0
        (JNIEnv *env, jobject obj, jstring jfilename) {
    throw std::runtime_error("ColStore::loadtrie Not yet implemented");
}


JNIEXPORT jobjectArray JNICALL Java_backend_ColumnStoreCBackend_prepareFromTrie0
        (JNIEnv *env, jobject obj, jintArray jquery) {
    throw std::runtime_error("ColStore::prepareFromTrie Not yet implemented");
}

JNIEXPORT jint JNICALL Java_backend_ColumnStoreCBackend_mkAll0
        (JNIEnv *env, jobject obj, jint numCols, jint numRows) {
    return colStore.mkAll(numCols, numRows);
}

JNIEXPORT jint JNICALL Java_backend_ColumnStoreCBackend_mk0
        (JNIEnv *env, jobject obj, jint numCols) {
    return colStore.mk(numCols);
}


JNIEXPORT jint JNICALL Java_backend_ColumnStoreCBackend_sSize0
        (JNIEnv *env, jobject obj, jint s_id) {
    return colStore.numRowsInCuboid(s_id);
}


JNIEXPORT jlong JNICALL Java_backend_ColumnStoreCBackend_sNumBytes0
        (JNIEnv *env, jobject obj, jint s_id) {
    return colStore.numBytesInSparseCuboid(s_id);
}

JNIEXPORT jlongArray JNICALL Java_backend_ColumnStoreCBackend_dFetch0
        (JNIEnv *env, jobject obj, jint d_id) {
    size_t numRows;
    auto ptr = (jlong *) colStore.fetch(d_id, numRows);

    jlongArray result = env->NewLongArray(numRows);
    env->SetLongArrayRegion(result, 0, numRows, ptr);
    return result;
}

JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_sFetch640
        (JNIEnv *env, jobject obj, jint s_id, jint word_id, jobject byteBuffer) {
    void* ptr = env->GetDirectBufferAddress(byteBuffer);
    colStore.fetch64Sparse(s_id, word_id, ptr);
}

JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_cuboidGC0
        (JNIEnv *env, jobject obj, jint id) {
    colStore.unloadCuboid(id);
}


JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_add_1i
        (JNIEnv *env, jobject obj, jint startIdx, jint s_id, jint numCols, jint numRecords, jobject jbytebuffer) {
    auto records = (const byte **) env->GetDirectBufferAddress(jbytebuffer);
    SparseCuboidRow rowsToAdd(records, numRecords, numCols);
    colStore.addRowsAtPosition(startIdx, s_id, rowsToAdd);
}


JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_add
        (JNIEnv *env, jobject obj, jint s_id, jint numCols, jint numRecords, jobject jbytebuffer) {
    auto records = (byte * ) env->GetDirectBufferAddress(jbytebuffer);
    SparseCuboidRow rowsToAdd (records, numRecords, numCols);
    colStore.addRowsToCuboid(s_id, rowsToAdd);
}




JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_freeze
        (JNIEnv *env, jobject obj, jint s_id) {
    colStore.freeze(s_id);
}


JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_freezeMkAll
        (JNIEnv *env, jobject obj, jint s_id) {
    colStore.freezeMkAll(s_id);
}

JNIEXPORT jintArray JNICALL Java_backend_ColumnStoreCBackend_readMultiCuboid0
        (JNIEnv *env, jobject obj, jstring jfilename, jbooleanArray jIsSparse, jintArray jNumCols, jintArray jNumRows) {
    jsize numCuboids = env->GetArrayLength(jIsSparse);
    const char *filename = env->GetStringUTFChars(jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(jIsSparse, 0);
    jint *numColsArray = env->GetIntArrayElements(jNumCols, 0);
    jint *numRowsArray = env->GetIntArrayElements(jNumRows, 0);
    unsigned int* idsArray = new unsigned int[numCuboids];
    colStore.readMultiCuboid(filename, numColsArray, numRowsArray, isSparseArray, idsArray, numCuboids);
    jintArray jids = env->NewIntArray(numCuboids);
    env->SetIntArrayRegion(jids, 0, numCuboids, (jint *) idsArray);
    env->ReleaseStringUTFChars(jfilename, filename);
    env->ReleaseBooleanArrayElements(jIsSparse, isSparseArray, 0);
    env->ReleaseIntArrayElements(jNumCols, numColsArray, 0);
    env->ReleaseIntArrayElements(jNumRows, numRowsArray, 0);
    delete[] idsArray;
    return jids;
}


JNIEXPORT void JNICALL Java_backend_ColumnStoreCBackend_writeMultiCuboid0
        (JNIEnv *env, jobject obj, jstring jfilename, jbooleanArray jIsSparse, jintArray jIds) {
    jsize numCuboids = env->GetArrayLength(jIsSparse);
    const char *filename = env->GetStringUTFChars(jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(jIsSparse, 0);
    jint *idArray = env->GetIntArrayElements(jIds, 0);
    colStore.writeMultiCuboid(filename, isSparseArray, idArray, numCuboids);
    env->ReleaseStringUTFChars(jfilename, filename);
    env->ReleaseBooleanArrayElements(jIsSparse, isSparseArray, 0);
    env->ReleaseIntArrayElements(jIds, idArray, 0);
}
