#include <iostream>
#include <cstdio>
#include "SetTrie.h"

using namespace std;

bool verifyQueryTest1(const SetTrie &trie, const vector<int> &query) {
    map<int, value_t> moments;
    cout << "Query ";
    for (int q: query) cout << q << " ";

    trie.getNormalizedSubset(query, moments, 0, 0, trie.nodes);
    size_t N = 1 << query.size();
    if (moments.size() != N) {
        fprintf(stdout, " FAILED Obtained %lu moments, expected %lu \n", moments.size(), N);
        return false;
    }
    for (size_t i = 0; i < N; i++) {
        if (((int) moments[i] - 1) != pupInt(query, i)) {
            fprintf(stdout, " FAILED Obtained %lld for moments[%lu], expected %lu  \n", moments[i], i, i + 1);
            return false;
        }
    }
    cout << " PASSED" << endl;
    return true;
}

bool test1() {
    SetTrie trie;
    trie.init(1024);
    int nbits = 10;
    int N = 1 << nbits;
    int keyArray[nbits];
    for (int i = 0; i < N; i++) {
        int len = bitsFromInt(i, keyArray);
        vector<int> keyVector(keyArray, keyArray + len);
        trie.insert(keyVector, i + 1);
    }
    if (trie.count != N) {
        cerr << "Count = " << trie.count << " Expected value " << N << endl;
        return false;
    }

    vector<int> q1;
    vector<int> q2;
    for (int i = 0; i < nbits; i++) q2.push_back(i);
    vector<int> q3 = {5, 7, 9};
    vector<int> q4 = {2, 5, 6};
    vector<int> q5 = {0, 5, 6, 7};
    vector<int> q6 = {0, 2, 3, 4, 8};
    vector<int> q7 = {1, 3, 4};

    bool result = true;
    result &= verifyQueryTest1(trie, q1);
    result &= verifyQueryTest1(trie, q2);
    result &= verifyQueryTest1(trie, q3);
    result &= verifyQueryTest1(trie, q4);
    result &= verifyQueryTest1(trie, q5);
    result &= verifyQueryTest1(trie, q6);
    result &= verifyQueryTest1(trie, q7);
    if (result) cout << "Test1 SUCCESS" << endl;
    else cout << "Test1 FAILURE" << endl;
    return result;

}

bool test2() {
    SetTrie trie;
    trie.init(100);

    vector<int> c1 = {0, 1};
    vector<value_t> v1 = {7, 3, 6, 1};
    momentTransform(v1);
    trie.insertAll(c1, v1);

    vector<int> c2 = {1, 2};
    vector<value_t> v2 = {1, 4, 9, 3};
    momentTransform(v2);
    trie.insertAll(c2, v2);

    vector<int> c3 = {0, 2};
    vector<value_t> v3 = {3, 2, 10, 2};
    momentTransform(v3);
    trie.insertAll(c3, v3);

    if (trie.count != 7) {
        cout << "Trie count does not match. Expected 7 Obtained " << trie.count << endl;
        return false;
    }

    char filename[] = "triemomentTest.trie";
    trie.saveToFile(filename);


    SetTrie trie2;
    trie2.loadFromFile(filename);
    if (trie2.count != 7) {
        cout << "Trie2 count does not match. Expected 7 Obtained " << trie2.count << endl;
        return false;
    }
    vector<int> query = {0, 1, 2};
    vector<value_t> actual = {17, 4, 7, 1, 12, 2, 3};
    map<int, value_t> result;
    trie.getNormalizedSubset(query, result, 0, 0, trie.nodes);
    if (result.size() != 7) {
        cout << "Moment results expected 7, obtained " << result.size() << endl;
        return false;
    }
    for (auto kv: result) {
        int k = kv.first;
        value_t v = kv.second;
        value_t v2 = actual[k];
        if (v != v2) {
            printf("Moment[%d] does not match. Expected %lld, obtained %lld \n", k, v2, v);
            return false;
        }
    }
    remove(filename);
    cout << "Test 2 SUCCESS" << endl;
    return true;
}

bool test3() {
    SetTrie trie;
    trie.init(1024);

    vector<int> c3 = {0, 2};
    vector<value_t> v3 = {3, 2, 10, 2};
    momentTransform(v3);
    trie.insertAll(c3, v3);

    vector<int> query = {0, 1, 2};
    map<int, value_t> result;
    trie.getNormalizedSubset(query, result, 0, 0, trie.nodes);

    bool flag = true;
    if(result.size() != 4) {
        cout << "Moment results expected 7, obtained " << result.size() << endl;
        flag = false;
    }
    if(result[0] != 17) {
        printf("Moment[0] does not match. Expected 17, obtained %lu \n",  result[0]);
        flag = false;
    }
    if(result[4] != 12) {
        printf("Moment[4] does not match. Expected 12, obtained %lu \n",  result[4]);
        flag = false;
    }
    if(result[1] != 4) {
        printf("Moment[1] does not match. Expected 4, obtained %lu \n",  result[1]);
        flag = false;
    }
    if(result[5] != 2) {
        printf("Moment[5] does not match. Expected 2, obtained %lu \n",  result[5]);
        flag = false;
    }
    if(flag) cout << "Test 3 SUCCESS" << endl;
    return flag;
}

int main(int argc, char **argv) {
    bool result = true;
    result &= test1();
    result &= test2();
    result &= test3();
    result &= test4();
    if (result) cout << "ALL TESTS PASSED " << endl;
    else cout << "SOME TESTS FAILED " << endl;

}