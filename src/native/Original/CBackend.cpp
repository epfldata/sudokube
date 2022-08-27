#include<assert.h>
#include<stdio.h>
#include "backend_OriginalCBackend.h"
#include "Keys.h"
#include "SetTrie.h"

extern void readRegistry(unsigned int id, void *&ptr, size_t &size, unsigned short &keySize);

extern void reset();

extern int shybridhash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);

extern unsigned int srehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);

extern unsigned int s2drehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);

extern unsigned int drehash(unsigned int d_id, unsigned int *maskpos, unsigned int masksum);

extern value_t *s2drehashSlice(unsigned int s_id, unsigned int *bitpos, unsigned int bitlength, bool *sliceMask);

extern value_t *drehashSlice(unsigned int d_id, unsigned int *bitpos, unsigned int bitposlength, bool *sliceMask);

extern unsigned int mkAll(unsigned int n_bits, size_t n_rows);

extern unsigned int mk(unsigned int n_bits);

extern void add_i(size_t i, unsigned int s_id, byte *key, value_t v);

extern void add(unsigned int s_id, unsigned int n_bits, byte *key, value_t v);

extern void freeze(unsigned int s_id);

extern void freezePartial(unsigned int s_id, unsigned int n_bits);

extern value_t *fetch(unsigned int d_id, size_t &size);

extern void cuboid_GC(unsigned int id);

extern size_t sz(unsigned int id);

extern size_t sNumBytes(unsigned int id);

extern bool addDenseCuboidToTrie(const std::vector<int> &cuboidDims, unsigned int d_id);

extern bool addSparseCuboidToTrie(const std::vector<int> &cuboidDims, unsigned int s_id);

extern void loadTrie(const char *filename);

extern void saveTrie(const char *filename);

extern void initTrie(size_t ms);

extern void prepareFromTrie(const std::vector<int> &query, std::map<int, value_t> &result);

extern void readMultiCuboid(const char *filename, int n_bits_array[], int size_array[], unsigned char isSparse_array[],
                            unsigned int id_array[], unsigned int numCuboids);

extern void writeMultiCuboid(const char *filename, unsigned char isSparse_array[], int ids[], unsigned int numCuboids);


JNIEXPORT void JNICALL Java_backend_OriginalCBackend_reset0(JNIEnv *env, jobject obj) {
    reset();
}

JNIEXPORT jintArray JNICALL Java_backend_OriginalCBackend_readMultiCuboid0
        (JNIEnv *env, jobject obj, jstring Jfilename, jbooleanArray JisSparseArray, jintArray JnbitsArray,
         jintArray JsizeArray) {
    jsize numCuboids = env->GetArrayLength(JisSparseArray);
    const char *filename = env->GetStringUTFChars(Jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(JisSparseArray, 0);
    jint *nbitsArray = env->GetIntArrayElements(JnbitsArray, 0);
    jint *sizeArray = env->GetIntArrayElements(JsizeArray, 0);
    unsigned int idsArray[numCuboids];
    readMultiCuboid(filename, nbitsArray, sizeArray, isSparseArray, idsArray, numCuboids);
    jintArray result = env->NewIntArray(numCuboids);
    if (result == NULL) return NULL; // out of memory error thrown
    env->SetIntArrayRegion(result, 0, numCuboids, (jint *) idsArray);
    env->ReleaseStringUTFChars(Jfilename, filename);
    env->ReleaseBooleanArrayElements(JisSparseArray, isSparseArray, 0);
    env->ReleaseIntArrayElements(JnbitsArray, nbitsArray, 0);
    env ->ReleaseIntArrayElements(JsizeArray, sizeArray, 0);
    return result;

}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_writeMultiCuboid0
        (JNIEnv *env, jobject obj, jstring Jfilename, jbooleanArray JisSparseArray, jintArray JidArray) {
    jsize numCuboids = env->GetArrayLength(JisSparseArray);
    const char *filename = env->GetStringUTFChars(Jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(JisSparseArray, 0);
    jint *idArray = env->GetIntArrayElements(JidArray, 0);
    assert(sizeof(jint) == sizeof(unsigned int));
    writeMultiCuboid(filename, isSparseArray, idArray, numCuboids);
    env->ReleaseStringUTFChars(Jfilename, filename);
    env->ReleaseBooleanArrayElements(JisSparseArray, isSparseArray, 0);
    env->ReleaseIntArrayElements(JidArray, idArray, 0);
}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_add_1i
        (JNIEnv *env, jobject obj, jint idx, jint s_id, jint n_bits, jbyteArray key, jlong v) {
    jsize keylen = env->GetArrayLength(key);
    auto ckey =  env->GetByteArrayElements(key, 0);
    // the key is transmitted as an array of signed bytes (should be read as unsigned bytes)
    assert(n_bits <= keylen * 8);
    add_i(idx, s_id, (byte *) ckey, v);
    env->ReleaseByteArrayElements(key, ckey, 0);
}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_add
        (JNIEnv *env, jobject obj, jint s_id, jint n_bits, jbyteArray key, jlong v) {
    jsize keylen = env->GetArrayLength(key);
    jbyte *keybody = env->GetByteArrayElements(key, 0);

    // the key is transmitted as an array of bytes (0 to 255).

    add(s_id, n_bits, (byte *) keybody, v);

    env->ReleaseByteArrayElements(key, keybody, 0);
}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_freeze
        (JNIEnv *env, jobject obj, jint s_id) {
    freeze(s_id);
}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_freezePartial
        (JNIEnv *env, jobject obj, jint s_id, jint n_bits) {
    freezePartial(s_id, n_bits);
}


JNIEXPORT jint JNICALL Java_backend_OriginalCBackend_sSize0
        (JNIEnv *env, jobject obj, jint id) {
    return sz(id);
}

JNIEXPORT jlong JNICALL Java_backend_OriginalCBackend_sNumBytes0
        (JNIEnv *, jobject, jint id) {
    return sNumBytes(id);
}

JNIEXPORT jint JNICALL Java_backend_OriginalCBackend_mk0
        (JNIEnv *env, jobject obj, jint n_bits) {
    return mk(n_bits);
}

JNIEXPORT jint JNICALL Java_backend_OriginalCBackend_mkAll0
        (JNIEnv *env, jobject obj, jint n_bits, jint n_rows) {
    return mkAll(n_bits, n_rows);
}


JNIEXPORT jint JNICALL Java_backend_OriginalCBackend_sRehash0
        (JNIEnv *env, jobject obj, jint s_id, jintArray pos, jint mode) {
    jsize poslen = env->GetArrayLength(pos);
    jint *posbody = env->GetIntArrayElements(pos, 0);
    int x;
    if(mode == 1) x = s2drehash(s_id, (unsigned int *) posbody, poslen);
    else if(mode == 2)  x = srehash(s_id, (unsigned int *) posbody, poslen);
    else if(mode == 3) x = shybridhash(s_id, (unsigned int *) posbody, poslen);
    else throw std::runtime_error("Unknown mode for srehash");
    env->ReleaseIntArrayElements(pos, posbody, 0);
    return x;
}


JNIEXPORT jint JNICALL Java_backend_OriginalCBackend_dRehash0
        (JNIEnv *env, jobject obj, jint d_id, jintArray pos) {
    jsize poslen = env->GetArrayLength(pos);
    jint *posbody = env->GetIntArrayElements(pos, 0);

    int x = drehash(d_id, (unsigned int *) posbody, poslen);

    env->ReleaseIntArrayElements(pos, posbody, 0);
    return x;
}


JNIEXPORT jlongArray JNICALL Java_backend_OriginalCBackend_sRehashSlice0
        (JNIEnv *env, jobject obj, jint s_id, jintArray jbitpos, jbooleanArray jslicemask) {
    jsize bitposlen = env->GetArrayLength(jbitpos);
    jint *bitpos = env->GetIntArrayElements(jbitpos, 0);
    jboolean *sliceMask = env->GetBooleanArrayElements(jslicemask, 0);
    jsize slicemasklen = env->GetArrayLength(jslicemask);
    assert(slicemasklen == (1 << bitposlen));
    value_t *result = s2drehashSlice(s_id, (unsigned int *) bitpos, bitposlen, (bool *) sliceMask);
    env->ReleaseIntArrayElements(jbitpos, bitpos, 0);
    env->ReleaseBooleanArrayElements(jslicemask, sliceMask, 0);
    jlongArray jresult = env->NewLongArray(slicemasklen);
    if (result == NULL) return NULL; // out of memory error thrown
    jlong *fill;
    if (sizeof(jlong) != sizeof(value_t))
        fill = nullptr;
    else
        fill = (jlong *) result;
    env->SetLongArrayRegion(jresult, 0, slicemasklen, fill);
    return jresult;
}

JNIEXPORT jlongArray JNICALL Java_backend_OriginalCBackend_dRehashSlice0
        (JNIEnv *env, jobject obj, jint d_id, jintArray jbitpos, jbooleanArray jslicemask) {
    jsize bitposlen = env->GetArrayLength(jbitpos);
    jint *bitpos = env->GetIntArrayElements(jbitpos, 0);
    jboolean *sliceMask = env->GetBooleanArrayElements(jslicemask, 0);
    jsize slicemasklen = env->GetArrayLength(jslicemask);
    assert(slicemasklen == (1 << bitposlen));
    value_t *result = drehashSlice(d_id, (unsigned int *) bitpos, bitposlen, (bool *) sliceMask);
    env->ReleaseIntArrayElements(jbitpos, bitpos, 0);
    env->ReleaseBooleanArrayElements(jslicemask, sliceMask, 0);
    jlongArray jresult = env->NewLongArray(slicemasklen);
    if (result == NULL) return NULL; // out of memory error thrown
    jlong *fill;
    if (sizeof(jlong) != sizeof(value_t))
        fill = nullptr;
    else
        fill = (jlong *) result;
    env->SetLongArrayRegion(jresult, 0, slicemasklen, fill);
    return jresult;
}


JNIEXPORT jlongArray JNICALL Java_backend_OriginalCBackend_dFetch0
        (JNIEnv *env, jobject obj, int d_id) {
    size_t size;
    value_t *p = fetch(d_id, size); // fetch does not copy, but in general, we
    // first build the cuboid #d_id just for this
    // fetch, and it's not deallocated. That's
    // kind of a mem leak.

    jlongArray result = env->NewLongArray(size);
    if (result == NULL) return NULL; // out of memory error thrown

    //Can't declare large array in stack. Stack out of bounds
    jlong *fill;
    if (sizeof(jlong) != sizeof(value_t))
        fill = nullptr;
    else
        fill = (jlong *) p;

    env->SetLongArrayRegion(result, 0, size, fill);
    return result;
}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_cuboidGC0
        (JNIEnv *env, jobject obj, int id) {
    unsigned cuboid_id = id < 0 ? -id : id;
    cuboid_GC(cuboid_id);
}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_saveAsTrie0
        (JNIEnv *env, jobject obj, jobjectArray array, jstring Jfilename, jlong maxSize) {
    jsize len = env->GetArrayLength(array);
    jobject first = env->GetObjectArrayElement(array, 0);
    jclass tuple2Class = env->GetObjectClass(first);
    jclass intClass = env->FindClass("Ljava/lang/Integer;");
    jmethodID intValue = env->GetMethodID(intClass, "intValue", "()I");
    jmethodID getFirst = env->GetMethodID(tuple2Class, "_1", "()Ljava/lang/Object;");
    jmethodID getSecond = env->GetMethodID(tuple2Class, "_2", "()Ljava/lang/Object;");

    initTrie(maxSize);
    const char *filename = env->GetStringUTFChars(Jfilename, 0);
    for (size_t i = 0; i < len; i++) {
        jobject tupleObject = env->GetObjectArrayElement(array, i);
        jintArray jcuboidDims = (jintArray) env->CallObjectMethod(tupleObject, getFirst);
        jobject secondObject = env->CallObjectMethod(tupleObject, getSecond);
        int cuboid_id = env->CallIntMethod(secondObject, intValue);
        jsize innerlen = env->GetArrayLength(jcuboidDims);
        int *cuboidDims = env->GetIntArrayElements(jcuboidDims, 0);
        std::vector<int> cuboidDimVec(cuboidDims, cuboidDims + innerlen);
        if (cuboid_id < 0) { //dense cuboid
            int d_id = -cuboid_id;
            if (!addDenseCuboidToTrie(cuboidDimVec, d_id)) break;
        } else {
            int s_id = cuboid_id;
            if (!addSparseCuboidToTrie(cuboidDimVec, s_id)) break;
        }
    }
    saveTrie(filename);
}

JNIEXPORT void JNICALL Java_backend_OriginalCBackend_loadTrie0
        (JNIEnv *env, jobject obj, jstring Jfilename) {
    const char *filename = env->GetStringUTFChars(Jfilename, 0);
    loadTrie(filename);
}

JNIEXPORT jobjectArray JNICALL Java_backend_OriginalCBackend_prepareFromTrie0
        (JNIEnv *env, jobject obj, jintArray array) {
    jsize len = env->GetArrayLength(array);
    jint *qarray = env->GetIntArrayElements(array, 0);
    std::vector<int> qvec(qarray, qarray + len);
    std::map<int, value_t> result;
    prepareFromTrie(qvec, result);
    size_t resultLen = result.size();

    jclass tuple2Class = env->FindClass("Lscala/Tuple2;");
    jmethodID tuple2init = env->GetMethodID(tuple2Class, "<init>", "(Ljava/lang/Object;Ljava/lang/Object;)V");

    jclass intClass = env->FindClass("Ljava/lang/Integer;");
    jmethodID intInit = env->GetMethodID(intClass, "<init>", "(I)V");

    jclass longClass = env->FindClass("Ljava/lang/Long;");
    jmethodID longInit = env->GetMethodID(longClass, "<init>", "(J)V");

    jobjectArray objArray = env->NewObjectArray(resultLen, tuple2Class, 0);
    size_t i = 0;

    for (auto kv: result) {
        jobject intObj = env->NewObject(intClass, intInit, kv.first);
        jobject longObj = env->NewObject(longClass, longInit, kv.second);
        jobject tupleObject = env->NewObject(tuple2Class, tuple2init, intObj, longObj);
        env->SetObjectArrayElement(objArray, i, tupleObject);
        i++;
    }
    return objArray;
}
