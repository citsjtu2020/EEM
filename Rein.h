#ifndef _REIN_H
#define _REIN_H
#include <cstring>
#include "Util.h"
#include "constant.h"
#include <algorithm>
#include <unordered_set>

// // kafka head file
// #include <stdio.h>
// #include <stdlib.h>
// #include <iostream>
// #include <list>
// #include <memory>
// #include <string>
// #include <string.h>
#include <queue>
// #include<time.h>
#include "kafkaConsumer.h"

#define _for(i,a,b) for( int i=(a); i<(b); ++i)
#define __for(i,a,b) for( int i=(a); i<=(b); ++i)
#define mfor(i,a,b) for(int i=(a);i>(b);--i)
#define mmfor(i,a,b) for(int i=(a);i>=(b);--i)

//const int MAX_SUBS = 2000001;
//const int MAX_ATTS = 101;
//const int MAX_BUCKS = 500;


class Rein {
	int numSub, numDimension, buckStep;
	vector<vector<vector<Combo>>> data[2];    // original Rein and AWRein 0:left parenthesis, 1:right parenthesis
	vector<vector<vector<IntervalCombo>>> fData[2]; // forward Rein (fRein)
//	vector<vector<vector<vector<IntervalCombo>>>> awData[2]; // l/h->attr->level->buck->i
	int subPredicate[subs]; // forward Rein; for AWRein, it means the number of predicates inserted to fData for forward matching
	int counter[subs]; // forward Rein
	vector<bitset<subs>> nB; // null bitset for forward Rein with C-BOMP
	vector<bitset<subs>> nnB; // non-null bitset for backward matching, same as HEM

public:
	int numBucket;
	double compareTime = 0.0; // 
	double markTime = 0.0;    // 
	double bitTime = 0.0;     // 
	vector<unordered_set<int>> bucketSub;   // 

	Rein(int);

	// Original Rein
	//void insert(Sub sub);
	void insert_backward_original(IntervalSub sub);
	//void match(const Pub& pub, int& matchSubs, const vector<Sub>& subList);
	void match_backward_original(const Pub& pub, int& matchSubs);
	bool deleteSubscription_backward_original(IntervalSub sub);
	int calMemory_backward_original();     // 
    //kafak
    void run_Rein_Original_consumer(const intervalGenerator &gen);

	// Forward Rein
	void insert_forward_native(IntervalSub sub);
	void match_forward_native(const Pub& pub, int& matchSubs);
	bool deleteSubscription_forward_native(IntervalSub sub);
	int calMemory_forward_native();

	// Forward Rein with C-BOMP
	void insert_forward_CBOMP(IntervalSub sub);
	void match_forward_CBOMP(const Pub& pub, int& matchSubs);
	bool deleteSubscription_forward_CBOMP(IntervalSub sub);
	int calMemory_forward_CBOMP();



	// HybridRein (AWRein)
	void insert_hybrid_native(IntervalSub sub);
	void match_hybrid_native(const Pub& pub, int& matchSubs);
	bool deleteSubscription_hybrid_native(IntervalSub sub);
	int calMemory_hybrid_native();

	// HybridRein (AWRein) with C-BOMP
	void insert_hybrid_CBOMP(IntervalSub sub);
	void match_hybrid_CBOMP(const Pub& pub, int& matchSubs);
	bool deleteSubscription_hybrid_CBOMP(IntervalSub sub);
	int calMemory_hybrid_CBOMP();

	void calBucketSize(); // 
	vector<int> calMarkNumForBuckets(); // 
};

// 
//class Rein {
//	int numSub, numDimension, buckStep, numBucket; // ά�ȣ�Ͱ���ȣ�Ͱ��
//	vector<vector<vector<vector<Combo>>>> data;    // 0:left parenthesis, 1:right parenthesis
//
//public:
//	Rein(int valDom, int numSubscription, int numDim, int numBuck) :numSub(numSubscription), numDimension(numDim) {
//		buckStep = (valDom - 1) / numBuck + 1;
//		numBucket = (valDom - 1) / buckStep + 1;
//		cout << "Rein: bucketStep = " << buckStep << ", numBucket = " << numBucket << endl;
//		data.resize(numDimension, vector<vector<vector<Combo>>>(2, vector<vector<Combo>>(numBucket)));
//	}
//	//void insert(Sub sub);
//	void insert(IntervalSub sub);
//	//void match(const Pub& pub, int& matchSubs, const vector<Sub>& subList);
//	void match(const Pub& pub, int& matchSubs);
//};

// 
//class Rein {
//	int numSub, numDimension, buckStep, numBucket; // 
//	vector<Combo> data[MAX_ATTS][2][MAX_BUCKS];
//
//public:
//	Rein(int valDom, int numSubscription, int numDim, int numBuck) :numSub(numSubscription), numDimension(numDim) {
//		buckStep = (valDom - 1) / numBuck + 1;
//		numBucket = (valDom - 1) / buckStep + 1;
//		cout << "Rein: bucketStep = " << buckStep << ", numBucket = " << numBucket << endl;
//		//data.resize(numDimension, vector<vector<vector<Combo>>>(2, vector<vector<Combo>>(numBucket)));
//	}
//	//void insert(Sub sub);
//	void insert(IntervalSub sub);
//	//void match(const Pub& pub, int& matchSubs, const vector<Sub>& subList);
//	void match(const Pub& pub, int& matchSubs);
//};

#endif
