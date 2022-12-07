#include "backend_RowStoreCBackend.h"
#include "RowStore.h"

RowStore rowStore;

JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_reset0
        (JNIEnv *env, jobject obj) {
    rowStore.clear();
}

JNIEXPORT jint JNICALL Java_backend_RowStoreCBackend_sRehash0
        (JNIEnv *env, jobject obj, jint s_id, jintArray jbitpos, jint mode) {
    jsize bitposlen = env->GetArrayLength(jbitpos);
    auto bitpos = env->GetIntArrayElements(jbitpos, 0);
    std::vector<unsigned int> bitposvec(bitpos, bitpos + bitposlen);
    auto newid = rowStore.srehash(s_id, bitposvec, mode);
    env->ReleaseIntArrayElements(jbitpos, bitpos, 0);
    return newid;
}


JNIEXPORT jint JNICALL Java_backend_RowStoreCBackend_dRehash0
        (JNIEnv *env, jobject obj, jint d_id, jintArray jbitpos) {
    jsize bitposlen = env->GetArrayLength(jbitpos);
    auto bitpos = env->GetIntArrayElements(jbitpos, 0);
    std::vector<unsigned int> bitposvec(bitpos, bitpos + bitposlen);
    auto newid = rowStore.drehash(d_id, bitposvec);
    env->ReleaseIntArrayElements(jbitpos, bitpos, 0);
    return newid;
}


JNIEXPORT jlongArray JNICALL Java_backend_RowStoreCBackend_sRehashSlice0
        (JNIEnv *env, jobject obj, jint s_id, jintArray jbitpos, jbooleanArray jslicemask) {
    throw std::runtime_error("RowStore::sRehashSlice0 Not yet implemented");
}


JNIEXPORT jlongArray JNICALL Java_backend_RowStoreCBackend_dRehashSlice0
        (JNIEnv *env, jobject obj, jint d_id, jintArray jbitpos, jbooleanArray jslicemask) {
    throw std::runtime_error("RowStore::dRehashSlice0 Not yet implemented");
}


JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_saveAsTrie0
        (JNIEnv *env, jobject obj, jobjectArray, jstring, jlong) {
    throw std::runtime_error("RowStore::saveAsTrie Not yet implemented");
}


JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_loadTrie0
        (JNIEnv *env, jobject obj, jstring jfilename) {
    throw std::runtime_error("RowStore::loadtrie Not yet implemented");
}


JNIEXPORT jobjectArray JNICALL Java_backend_RowStoreCBackend_prepareFromTrie0
        (JNIEnv *env, jobject obj, jintArray jquery) {
    throw std::runtime_error("RowStore::prepareFromTrie Not yet implemented");
}

JNIEXPORT jint JNICALL Java_backend_RowStoreCBackend_mkAll0
        (JNIEnv *env, jobject obj, jint numCols, jint numRows) {
    return rowStore.mkAll(numCols, numRows);
}

JNIEXPORT jint JNICALL Java_backend_RowStoreCBackend_mk0
        (JNIEnv *env, jobject obj, jint numCols) {
    return rowStore.mk(numCols);
}


JNIEXPORT jint JNICALL Java_backend_RowStoreCBackend_sSize0
        (JNIEnv *env, jobject obj, jint s_id) {
    return rowStore.numRowsInCuboid(s_id);
}


JNIEXPORT jlong JNICALL Java_backend_RowStoreCBackend_sNumBytes0
        (JNIEnv *env, jobject obj, jint s_id) {
    return rowStore.numBytesInSparseCuboid(s_id);
}

JNIEXPORT jlongArray JNICALL Java_backend_RowStoreCBackend_dFetch0
        (JNIEnv *env, jobject obj, jint d_id) {
    size_t numRows;
    auto ptr = (jlong *) rowStore.fetch(d_id, numRows);

    jlongArray result = env->NewLongArray(numRows);
    env->SetLongArrayRegion(result, 0, numRows, ptr);
    return result;
}




JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_add_1i
        (JNIEnv *env, jobject obj, jint startIdx, jint s_id, jint numCols, jint numRecords, jobject jbytebuffer) {
    auto records = (const byte **) env->GetDirectBufferAddress(jbytebuffer);
    SparseCuboidRow rowsToAdd(records, numRecords, numCols);
    rowStore.addRowsAtPosition(startIdx, s_id, rowsToAdd);
}


JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_add
        (JNIEnv *env, jobject obj, jint s_id, jint numCols, jint numRecords, jobject jbytebuffer) {
    auto records = (byte *) env->GetDirectBufferAddress(jbytebuffer);
    SparseCuboidRow rowsToAdd(records, numRecords, numCols);
    rowStore.addRowsToCuboid(s_id, rowsToAdd);
}


JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_freezePartial
        (JNIEnv *env, jobject obj, jint s_id, jint numCols) {
    rowStore.freezePartial(s_id, numCols);
}


JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_freeze
        (JNIEnv *env, jobject obj, jint s_id) {
    rowStore.freeze(s_id);
}


JNIEXPORT jintArray JNICALL Java_backend_RowStoreCBackend_readMultiCuboid0
        (JNIEnv *env, jobject obj, jstring jfilename, jbooleanArray jIsSparse, jintArray jNumCols, jintArray jNumRows) {
    jsize numCuboids = env->GetArrayLength(jIsSparse);
    const char *filename = env->GetStringUTFChars(jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(jIsSparse, 0);
    jint *numColsArray = env->GetIntArrayElements(jNumCols, 0);
    jint *numRowsArray = env->GetIntArrayElements(jNumRows, 0);
    unsigned int* idsArray = new unsigned int[numCuboids];
    rowStore.readMultiCuboid(filename, numColsArray, numRowsArray, isSparseArray, idsArray, numCuboids);
    jintArray jids = env->NewIntArray(numCuboids);
    env->SetIntArrayRegion(jids, 0, numCuboids, (jint *) idsArray);
    env->ReleaseStringUTFChars(jfilename, filename);
    env->ReleaseBooleanArrayElements(jIsSparse, isSparseArray, 0);
    env->ReleaseIntArrayElements(jNumCols, numColsArray, 0);
    env->ReleaseIntArrayElements(jNumRows, numRowsArray, 0);
    delete[] idsArray;
    return jids;
}


JNIEXPORT void JNICALL Java_backend_RowStoreCBackend_writeMultiCuboid0
        (JNIEnv *env, jobject obj, jstring jfilename, jbooleanArray jIsSparse, jintArray jIds) {
    jsize numCuboids = env->GetArrayLength(jIsSparse);
    const char *filename = env->GetStringUTFChars(jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(jIsSparse, 0);
    jint *idArray = env->GetIntArrayElements(jIds, 0);
    rowStore.writeMultiCuboid(filename, isSparseArray, idArray, numCuboids);
    env->ReleaseStringUTFChars(jfilename, filename);
    env->ReleaseBooleanArrayElements(jIsSparse, isSparseArray, 0);
    env->ReleaseIntArrayElements(jIds, idArray, 0);
}
