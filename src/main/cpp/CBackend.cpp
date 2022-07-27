#include<assert.h>
#include<stdio.h>
#include "backend_CBackend.h"
#include "Keys.h"
#include "SetTrie.h"

extern void reset();
extern  int   shybridhash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
extern unsigned int   srehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
extern unsigned int d2srehash(unsigned int d_id, unsigned int *maskpos, unsigned int masksum);
extern unsigned int s2drehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum);
extern unsigned int   drehash(unsigned int d_id, unsigned int *maskpos, unsigned int masksum);

extern unsigned int      mkAll(unsigned int n_bits, size_t n_rows);
extern unsigned int      mk(unsigned int n_bits);
extern void     add_i(size_t i, unsigned int s_id,  byte *key, value_t v);
extern void     add(unsigned int s_id, unsigned int n_bits, byte *key, value_t v);
extern void     freeze(unsigned int s_id);
extern void     freezePartial(unsigned int s_id, unsigned int n_bits);
extern value_t *fetch(unsigned int d_id, size_t& size);
extern void cuboid_GC(unsigned int id);
extern size_t      sz(unsigned int id);
extern size_t   sNumBytes(unsigned int id);

extern unsigned int   readSCuboid(const char *filename, unsigned int n_bits, size_t size);
extern unsigned int   readDCuboid(const char *filename, unsigned int n_bits, size_t size);
extern void writeSCuboid(const char *filename, unsigned int s_id);
extern void writeDCuboid(const char *filename, unsigned int d_id);

extern bool addDenseCuboidToTrie(const vector<int> &cuboidDims, unsigned int d_id);
extern bool addSparseCuboidToTrie(const vector<int> &cuboidDims, unsigned int s_id);
extern void loadTrie(const char* filename);
extern void saveTrie(const char* filename);
extern void initTrie(size_t ms);
extern void prepareFromTrie(const vector<int>& query, map<int, value_t> &result);

extern void readMultiCuboid(const char *filename,  int n_bits_array[], int size_array[], unsigned char isSparse_array[], unsigned int id_array[], unsigned int numCuboids);
extern void writeMultiCuboid(const char *filename, unsigned char isSparse_array[],  int ids[], unsigned int numCuboids);

JNIEXPORT void JNICALL Java_backend_CBackend_reset0(JNIEnv *env, jobject obj) {
    reset();
}

JNIEXPORT jintArray JNICALL Java_backend_CBackend_readMultiCuboid0
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
    return result;

}

JNIEXPORT void JNICALL Java_backend_CBackend_writeMultiCuboid0
        (JNIEnv *env, jobject obj, jstring Jfilename, jbooleanArray JisSparseArray, jintArray JidArray) {
    jsize numCuboids = env->GetArrayLength(JisSparseArray);
    const char *filename = env->GetStringUTFChars(Jfilename, 0);
    jboolean *isSparseArray = env->GetBooleanArrayElements(JisSparseArray, 0);
    jint *idArray = env->GetIntArrayElements(JidArray, 0);
    assert(sizeof(jint) == sizeof(unsigned int));
    writeMultiCuboid(filename, isSparseArray, idArray, numCuboids);
}

JNIEXPORT void JNICALL Java_backend_CBackend_add_1i
        (JNIEnv *env, jobject obj, jint idx, jint s_id, jint n_bits, jintArray key, jlong v) {
    jsize keylen = env->GetArrayLength(key);
    jint *keybody = env->GetIntArrayElements(key, 0);

    // the key is transmitted as an array of bytes (0 to 255).

    assert(n_bits <= keylen * 8);
    byte ckey[keylen];
    for (int i = 0; i < keylen; i++) ckey[i] = keybody[i];

    add_i(idx, s_id, ckey, v);

    env->ReleaseIntArrayElements(key, keybody, 0);
}

JNIEXPORT void JNICALL Java_backend_CBackend_add
        (JNIEnv *env, jobject obj, jint s_id, jint n_bits, jintArray key, jlong v) {
    jsize keylen = env->GetArrayLength(key);
    jint *keybody = env->GetIntArrayElements(key, 0);

    // the key is transmitted as an array of bytes (0 to 255).

    assert(n_bits <= keylen * 8);
    byte ckey[keylen];
    for (int i = 0; i < keylen; i++) ckey[i] = keybody[i];

    add(s_id, n_bits, ckey, v);

    env->ReleaseIntArrayElements(key, keybody, 0);
}

JNIEXPORT void JNICALL Java_backend_CBackend_freeze
        (JNIEnv *env, jobject obj, jint s_id) {
    freeze(s_id);
}

JNIEXPORT void JNICALL Java_backend_CBackend_freezePartial
  (JNIEnv* env, jobject obj, jint s_id, jint n_bits)
  {
    freezePartial(s_id, n_bits);
  }


JNIEXPORT jint JNICALL Java_backend_CBackend_sSize0
        (JNIEnv *env, jobject obj, jint id) {
    return sz(id);
}

JNIEXPORT jlong JNICALL Java_backend_CBackend_sNumBytes0
        (JNIEnv *, jobject, jint id) {
    return sNumBytes(id);
}

JNIEXPORT jint JNICALL Java_backend_CBackend_mk0
        (JNIEnv *env, jobject obj, jint n_bits) {
    return mk(n_bits);
}

JNIEXPORT jint JNICALL Java_backend_CBackend_mkAll0
        (JNIEnv *env, jobject obj, jint n_bits, jint n_rows) {
    return mkAll(n_bits, n_rows);
}

JNIEXPORT jint JNICALL Java_backend_CBackend_shhash
        (JNIEnv *env, jobject obj, jint s_id, jintArray pos) {
    jsize poslen = env->GetArrayLength(pos);
    jint *posbody = env->GetIntArrayElements(pos, 0);
    int id = shybridhash(s_id, (unsigned int *) posbody, poslen);
    env->ReleaseIntArrayElements(pos, posbody, 0);
    return id;

}

JNIEXPORT jint JNICALL Java_backend_CBackend_sRehash0
        (JNIEnv *env, jobject obj, jint s_id, jintArray pos) {
    jsize poslen = env->GetArrayLength(pos);
    jint *posbody = env->GetIntArrayElements(pos, 0);
    int x = srehash(s_id, (unsigned int *) posbody, poslen);

    env->ReleaseIntArrayElements(pos, posbody, 0);
    return x;
}

JNIEXPORT jint JNICALL Java_backend_CBackend_d2sRehash0
        (JNIEnv *env, jobject obj, jint d_id, jintArray pos) {
    jsize poslen = env->GetArrayLength(pos);
    jint *posbody = env->GetIntArrayElements(pos, 0);

    int x = d2srehash(d_id, (unsigned int *) posbody, poslen);

    env->ReleaseIntArrayElements(pos, posbody, 0);
    return x;
}

JNIEXPORT jint JNICALL Java_backend_CBackend_s2dRehash0
        (JNIEnv *env, jobject obj, jint d_id, jintArray pos) {
    jsize poslen = env->GetArrayLength(pos);
    jint *posbody = env->GetIntArrayElements(pos, 0);

    int x = s2drehash(d_id, (unsigned int *) posbody, poslen);

    env->ReleaseIntArrayElements(pos, posbody, 0);
    return x;
}

JNIEXPORT jint JNICALL Java_backend_CBackend_dRehash0
        (JNIEnv *env, jobject obj, jint d_id, jintArray pos) {
    jsize poslen = env->GetArrayLength(pos);
    jint *posbody = env->GetIntArrayElements(pos, 0);

    int x = drehash(d_id, (unsigned int *) posbody, poslen);

    env->ReleaseIntArrayElements(pos, posbody, 0);
    return x;
}


JNIEXPORT jlongArray JNICALL Java_backend_CBackend_dFetch0
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

JNIEXPORT void JNICALL Java_backend_CBackend_cuboidGC0
        (JNIEnv *env, jobject obj, int id) {
    unsigned cuboid_id = id < 0 ? -id : id;
    cuboid_GC(cuboid_id);
}

JNIEXPORT void JNICALL Java_backend_CBackend_saveAsTrie0
        (JNIEnv *env, jobject obj, jobjectArray array, jstring Jfilename, jlong maxSize) {
    jsize len = env->GetArrayLength(array);
    jobject first = env->GetObjectArrayElement(array, 0);
    jclass tuple2Class = env->GetObjectClass(first);
    jclass intClass = env->FindClass("Ljava/lang/Integer;");
    jmethodID  intValue = env->GetMethodID(intClass, "intValue", "()I");
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
        int* cuboidDims = env->GetIntArrayElements(jcuboidDims, 0);
        std::vector<int> cuboidDimVec(cuboidDims, cuboidDims + innerlen);
        if(cuboid_id < 0) { //dense cuboid
            int d_id = -cuboid_id;
            if(!addDenseCuboidToTrie(cuboidDimVec, d_id)) break;
        } else {
            int s_id = cuboid_id;
            if(!addSparseCuboidToTrie(cuboidDimVec, s_id)) break;
        }
    }
    saveTrie(filename);
}

JNIEXPORT void JNICALL Java_backend_CBackend_loadTrie0
        (JNIEnv *env, jobject obj, jstring Jfilename) {
    const char *filename = env->GetStringUTFChars(Jfilename, 0);
    loadTrie(filename);
}

JNIEXPORT jobjectArray JNICALL Java_backend_CBackend_prepareFromTrie0
        (JNIEnv *env, jobject obj, jintArray array) {
    jsize len = env->GetArrayLength(array);
    jint *qarray = env->GetIntArrayElements(array, 0);
    vector<int> qvec(qarray, qarray + len);
    map<int, value_t> result;
    prepareFromTrie(qvec, result);
    size_t resultLen =  result.size();

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
