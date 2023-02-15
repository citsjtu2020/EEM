#include "Rein.h"
//#include "librdkafka/rdkafkacpp.h"
//#include <time.h>
//#include <csignal>

Rein::Rein(int type) : numSub(0), numDimension(atts) {
	buckStep = (valDom - 1) / buks + 1;
	numBucket = (valDom - 1) / buckStep + 1;
	bucketSub.resize(numBucket);
	if (type == OriginalRein) { // Original Rein
		cout << "ExpID = " << expID << ". Rein: bucketStep = " << buckStep << ", numBucket = " << numBucket << endl;
		data[0].resize(numDimension, vector<vector<Combo>>(numBucket));
		data[1].resize(numDimension, vector<vector<Combo>>(numBucket));
	} else {
		memset(subPredicate, 0, sizeof(subPredicate));
		memset(counter, 0, sizeof(counter));
		fData[0].resize(numDimension, vector<vector<IntervalCombo>>(numBucket));
		fData[1].resize(numDimension, vector<vector<IntervalCombo>>(numBucket));
		if (type == ForwardRein) {
			cout << "ExpID = " << expID << ". Forward Rein (fRein): bucketStep = " << buckStep << ", numBucket = "
				 << numBucket << endl;
			//nnB.resize(atts); // fRein_to_bRein
		} else if (type == ForwardRein_CBOMP) {
			cout << "ExpID = " << expID << ". Forward Rein (fRein) with C-BOMP: bucketStep = " << buckStep
				 << ", numBucket = " << numBucket << endl;
			nB.resize(atts);
		} else {
			data[0].resize(numDimension, vector<vector<Combo>>(numBucket));
			data[1].resize(numDimension, vector<vector<Combo>>(numBucket));
			if (type == HybridRein) {
				cout << "ExpID = " << expID << ". HybridRein (AWRein): bucketStep = " << buckStep << ", numBucket = "
					 << numBucket << endl;
			} else if (type == HybridRein_CBOMP) {
				cout << "ExpID = " << expID << ". HybridRein (AWRein) with C-BOMP: bucketStep = " << buckStep
					 << ", numBucket = "
					 << numBucket << endl;
				nnB.resize(atts);
			}
		}
	}
}

//void Rein::insert(Sub sub)
//{
//    for (int i = 0; i < sub.size; i++)
//    {
//        Cnt cnt = sub.constraints[i];
//        Combo c;
//        c.val = cnt.value;
//        c.subID = sub.id;
//        if (cnt.op == 0)        // ==
//        {
//            data[0][cnt.att][c.val / buckStep].push_back(c);
//            data[1][cnt.att][c.val / buckStep].push_back(c);
//        }
//        else if (cnt.op == 1)   // >=
//            data[0][cnt.att][c.val / buckStep].push_back(c);
//        else                    // >=
//            data[1][cnt.att][c.val / buckStep].push_back(c);
//    }
//}

void Rein::insert_backward_original(IntervalSub sub) {
	for (int i = 0; i < sub.size; i++) {
		IntervalCnt cnt = sub.constraints[i];
		Combo c;
		// int bucketID = cnt.lowValue / buckStep; // Bug: ���ﱻ����
		c.val = cnt.lowValue;
		c.subID = sub.id;
		data[0][cnt.att][cnt.lowValue / buckStep].push_back(c);
		c.val = cnt.highValue;
		data[1][cnt.att][cnt.highValue / buckStep].push_back(c);
	}
	numSub++;
}

bool Rein::deleteSubscription_backward_original(IntervalSub sub) {
	int find = 0;
	IntervalCnt cnt;
	int bucketID, id = sub.id;

	_for(i, 0, sub.size) {
		cnt = sub.constraints[i];

		bucketID = cnt.lowValue / buckStep;
		vector<Combo>::iterator it;
		for (it = data[0][cnt.att][bucketID].begin(); it != data[0][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[0][cnt.att][bucketID].erase(it); // it = 
				find++;
				break;
			}

		bucketID = cnt.highValue / buckStep;
		for (it = data[1][cnt.att][bucketID].begin(); it != data[1][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[1][cnt.att][bucketID].erase(it); // it = 
				find++;
				break;
			}
	}
	if (find == 2 * sub.size)
		numSub--;
	return find == 2 * sub.size;
}
// 
//void Rein::match(const Pub& pub, int& matchSubs)
//{
//	vector<bool> bits(numSub, false);
//
//	for (int i = 0; i < pub.size; i++)
//	{
//		int value = pub.pairs[i].value, att = pub.pairs[i].att, buck = value / buckStep;
//		for (int k = 0; k < data[0][att][buck].size(); k++)
//			if (data[0][att][buck][k].val > value)
//				bits[data[0][att][buck][k].subID] = true;
//
//		for (int j = buck + 1; j < numBucket; j++)
//			for (int k = 0; k < data[0][att][j].size(); k++)
//				bits[data[0][att][j][k].subID] = true;
//
//
//		for (int k = 0; k < data[1][att][buck].size(); k++)
//			if (data[1][att][buck][k].val < value)
//				bits[data[1][att][buck][k].subID] = true;
//
//		for (int j = buck - 1; j >= 0; j--)
//			for (int k = 0; k < data[1][att][j].size(); k++)
//				bits[data[1][att][j][k].subID] = true;
//	}
//
//	for (int i = 0; i < numSub; i++)
//		if (!bits[i])
//			++matchSubs;
//}

// 
void Rein::match_backward_original(const Pub& pub, int& matchSubs)
{
	vector<bool> bits(numSub, false);
	vector<bool> attExist(numDimension, false);
	for (int i = 0; i < pub.size; i++)
	{
#ifdef DEBUG
		Timer compareStart;
#endif // DEBUG
		int value = pub.pairs[i].value, att = pub.pairs[i].att, buck = value / buckStep;
		attExist[att] = true;
		// ����������forѭ��ע���˾���ģ��ƥ��, ����Tama
		for (int k = 0; k < data[0][att][buck].size(); k++)
			if (data[0][att][buck][k].val > value)
				bits[data[0][att][buck][k].subID] = true;
		for (int k = 0; k < data[1][att][buck].size(); k++)
			if (data[1][att][buck][k].val < value)
				bits[data[1][att][buck][k].subID] = true;
#ifdef DEBUG
		compareTime += (double)compareStart.elapsed_nano();
		Timer markStart;
#endif // DEBUG
		for (int j = buck + 1; j < numBucket; j++)
			for (int k = 0; k < data[0][att][j].size(); k++)
				bits[data[0][att][j][k].subID] = true;
		for (int j = buck - 1; j >= 0; j--)
			for (int k = 0; k < data[1][att][j].size(); k++)
				bits[data[1][att][j][k].subID] = true;
#ifdef DEBUG
		markTime += (double)markStart.elapsed_nano();
#endif // DEBUG
	}
#ifdef DEBUG
	Timer markStart;
#endif // DEBUG
	for (int i = 0; i < numDimension; i++)
		if (!attExist[i])
			for (int j = 0; j < numBucket; j++)
				for (int k = 0; k < data[0][i][j].size(); k++)
					bits[data[0][i][j][k].subID] = true;
#ifdef DEBUG
	markTime += (double)markStart.elapsed_nano();
	Timer bitStart;
#endif // DEBUG
	for (int i = 0; i < subs; i++)
		if (!bits[i])
		{
			++matchSubs;
			//cout << "rein matches sub: " << i << endl;
		}
#ifdef DEBUG
	bitTime += (double)bitStart.elapsed_nano();
#endif // DEBUG
}
// 
//void Rein::match_backward_original(const Pub &pub, int &matchSubs) {
//	vector<bool> bits(numSub, false);
//	vector<bool> attExist(numDimension, false);
//	for (int i = 0; i < pub.size; i++) {
//		int value = pub.pairs[i].value, att = pub.pairs[i].att, buck = value / buckStep;
//		// cout<<"pubid= "<<pub.id<<" att= "<<att<<" value= "<<value<<endl;
//		attExist[att] = true;
//		// ����������forѭ��ע���˾���ģ��ƥ��, ����Tama
//		for (int k = 0; k < data[0][att][buck].size(); k++)
//			if (data[0][att][buck][k].val > value)
//				bits[data[0][att][buck][k].subID] = true;
//		for (int k = 0; k < data[1][att][buck].size(); k++)
//			if (data[1][att][buck][k].val < value)
//				bits[data[1][att][buck][k].subID] = true;
//
//		for (int j = buck + 1; j < numBucket; j++)
//			for (int k = 0; k < data[0][att][j].size(); k++)
//				bits[data[0][att][j][k].subID] = true;
//		for (int j = buck - 1; j >= 0; j--)
//			for (int k = 0; k < data[1][att][j].size(); k++)
//				bits[data[1][att][j][k].subID] = true;
//	}
//
//	for (int i = 0; i < numDimension; i++)
//		if (!attExist[i])
//			for (int j = 0; j < numBucket; j++)
//				for (int k = 0; k < data[0][i][j].size(); k++)
//					bits[data[0][i][j][k].subID] = true;
//
//	for (int i = 0; i < subs; i++)
//		if (!bits[i]) {
//			++matchSubs;
//			//cout << "rein matches sub: " << i << endl;
//		}
//}

int Rein::calMemory_backward_original() {
	long long size = sizeof(data[0]) * 2; // Byte
	_for(i, 0, numDimension) {
		//cout <<i<<": "<< sizeof(data[0][i]) << ": ";
		size += sizeof(data[0][i]) * 2;
		_for(j, 0, numBucket) {
			size += sizeof(data[0][i][j]) * 2 + sizeof(Combo) * (data[0][i][j].size() + data[1][i][j].size());
			//cout << sizeof(data[0][i][j]) << " ";
		}
		//cout << "\n";
	}
	size = size / 1024 / 1024; // MB
	return (int) size;
}



// ---------------------------------------------------------------------------------------------
// forward Rein (fRein)

void Rein::insert_forward_native(IntervalSub sub) {
	int bucketID;
	IntervalCombo c;
	c.subID = sub.id;
	subPredicate[sub.id] = sub.size;
	for (auto &&cnt: sub.constraints) {
		c.lowValue = cnt.lowValue;
		c.highValue = cnt.highValue;
		bucketID = cnt.lowValue / buckStep; // upper_bound probably is also OK!
		fData[0][cnt.att][bucketID].insert(
			lower_bound(fData[0][cnt.att][bucketID].begin(), fData[0][cnt.att][bucketID].end(), c,
						[&](const IntervalCombo &c1, const IntervalCombo &c2) {
							return c1.highValue == c2.highValue ? c1.lowValue < c2.lowValue : c1.highValue <
																							  c2.highValue;
						}), c); // insert ��������ǰ��!
		bucketID = cnt.highValue / buckStep;
		fData[1][cnt.att][bucketID].insert(
			lower_bound(fData[1][cnt.att][bucketID].begin(), fData[1][cnt.att][bucketID].end(), c,
						[&](const IntervalCombo &c1, const IntervalCombo &c2) {
							return c1.lowValue == c2.lowValue ? c1.highValue < c2.highValue : c1.lowValue < c2.lowValue;
						}), c);
	}
	numSub++;
}

void Rein::match_forward_native(const Pub &pub, int &matchSubs) {
	memcpy(counter, subPredicate, sizeof(subPredicate));
	int att, buck, midBuck = buks >> 1;
	IntervalCombo pubPredicateTmp;
	for (auto &&pair: pub.pairs) {
		pubPredicateTmp.lowValue = pubPredicateTmp.highValue = pair.value, att = pair.att, buck = pair.value / buckStep;
		// cout<<"pubid= "<<pub.id<<" att= "<<att<<" value= "<<value<<endl;
		if (buck < midBuck) { // Use low bucket list.
			const auto &&lowerBoundIterator = lower_bound(fData[0][att][buck].begin(), fData[0][att][buck].end(),
														  pubPredicateTmp,
														  [&](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.highValue < c2.highValue;
														  });
			for_each(lowerBoundIterator, fData[0][att][buck].end(), [&](const IntervalCombo &c) {
				if (c.lowValue <= pair.value) counter[c.subID]--;
			});

			for_each(fData[0][att].begin(), fData[0][att].begin() + buck, [&](const vector<IntervalCombo> &bucketList) {
				auto &&lowerBoundIterator = lower_bound(bucketList.begin(), bucketList.end(), pubPredicateTmp,
														[&](const IntervalCombo &c1, const IntervalCombo &c2) {
															return c1.highValue < c2.highValue;
														});
				for_each(lowerBoundIterator, bucketList.end(), [&](const IntervalCombo &c) { counter[c.subID]--; });
			});
		} else { // Use high bucket list.
			const auto &&upperBoundIterator = upper_bound(fData[1][att][buck].begin(), fData[1][att][buck].end(),
														  pubPredicateTmp,
														  [&](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.lowValue < c2.lowValue;
														  });
			for_each(fData[1][att][buck].begin(), upperBoundIterator, [&](const IntervalCombo &c) {
				if (c.highValue >= pair.value) counter[c.subID]--;
			});

			for_each(fData[1][att].begin() + buck + 1, fData[1][att].end(),
					 [&](const vector<IntervalCombo> &bucketList) {
						 auto &&upperBoundIterator = upper_bound(bucketList.begin(), bucketList.end(), pubPredicateTmp,
																 [&](const IntervalCombo &c1, const IntervalCombo &c2) {
																	 return c1.lowValue < c2.lowValue;
																 });
						 for_each(bucketList.begin(), upperBoundIterator,
								  [&](const IntervalCombo &c) { counter[c.subID]--; });
					 });
		}
	}

	for (int i = 0; i < subs; i++)
		if (counter[i] == 0) {
			++matchSubs;
			//cout << "rein matches sub: " << i << endl;
		}
}

bool Rein::deleteSubscription_forward_native(IntervalSub sub) { // δʵ��
	int find = 0;
	IntervalCnt cnt;
	int bucketID, id = sub.id;

	_for(i, 0, sub.size) {
		cnt = sub.constraints[i];

		bucketID = cnt.lowValue / buckStep;
		vector<Combo>::iterator it;
		for (it = data[0][cnt.att][bucketID].begin(); it != data[0][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[0][cnt.att][bucketID].erase(it); // it = 
				find++;
				break;
			}

		bucketID = cnt.highValue / buckStep;
		for (it = data[1][cnt.att][bucketID].begin(); it != data[1][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[1][cnt.att][bucketID].erase(it); // it = 
				find++;
				break;
			}
	}
	if (find == 2 * sub.size)
		numSub--;
	return find == 2 * sub.size;
}

int Rein::calMemory_forward_native() {
	long long size = sizeof(fData[0]) * 2; // Byte
	_for(i, 0, numDimension) {
		//cout <<i<<": "<< sizeof(data[0][i]) << ": ";
		size += sizeof(fData[0][i]) * 2;
		_for(j, 0, numBucket) {
			size += sizeof(fData[0][i][j]) + sizeof(fData[1][i][j]) +
					sizeof(IntervalCombo) * (fData[0][i][j].size() + fData[0][i][j].size());
			//cout << sizeof(data[0][i][j]) << " ";
		}
		//cout << "\n";
	}
	size += sizeof(subPredicate) + sizeof(counter);
	size = size / 1024 / 1024; // MB
	return (int) size;
}



// ---------------------------------------------------------------------------------------------
// forward Rein with C-BOMP (fRein_c)

void Rein::insert_forward_CBOMP(IntervalSub sub) {
	vector<bool> attrExist(atts, false);
	int bucketID;
	IntervalCombo c;
	c.subID = sub.id;
	subPredicate[sub.id] = sub.size;
	for (auto &&cnt: sub.constraints) {
		attrExist[cnt.att] = true;
		//nnB[cnt.att][sub.id] = 1;
		c.lowValue = cnt.lowValue;
		c.highValue = cnt.highValue;
		bucketID = cnt.lowValue / buckStep; // upper_bound probably is also OK!
		fData[0][cnt.att][bucketID].insert(
			lower_bound(fData[0][cnt.att][bucketID].begin(), fData[0][cnt.att][bucketID].end(), c,
						[&](const IntervalCombo &c1, const IntervalCombo &c2) {
							return c1.highValue == c2.highValue ? c1.lowValue < c2.lowValue : c1.highValue <
																							  c2.highValue;
						}), c); // insert ��������ǰ��!
		bucketID = cnt.highValue / buckStep;
		fData[1][cnt.att][bucketID].insert(
			lower_bound(fData[1][cnt.att][bucketID].begin(), fData[1][cnt.att][bucketID].end(), c,
						[&](const IntervalCombo &c1, const IntervalCombo &c2) {
							return c1.lowValue == c2.lowValue ? c1.highValue < c2.highValue : c1.lowValue < c2.lowValue;
						}), c);
	}
	_for(i, 0, atts) if (!attrExist[i])
			nB[i][sub.id] = 1;
	numSub++;
}

void Rein::match_forward_CBOMP(const Pub &pub, int &matchSubs) {
	bitset<subs> gB, mB; // global bitset
	gB.set(); // ȫ��Ϊ1, ���趼��ƥ���
	vector<bool> attExist(atts, false);
	int att, buck, midBuck = buks >> 1;
	IntervalCombo pubPredicateTmp;
	for (auto &&pair: pub.pairs) {
		mB = nB[pair.att];
		attExist[pair.att] = true;
		pubPredicateTmp.lowValue = pubPredicateTmp.highValue = pair.value, att = pair.att, buck = pair.value / buckStep;
		// cout<<"pubid= "<<pub.id<<" att= "<<att<<" value= "<<value<<endl;
		if (buck < midBuck) { // Use low bucket list.
			const auto &&lowerBoundIterator = lower_bound(fData[0][att][buck].begin(), fData[0][att][buck].end(),
														  pubPredicateTmp,
														  [&](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.highValue < c2.highValue;
														  });
			for_each(lowerBoundIterator, fData[0][att][buck].end(), [&](const IntervalCombo &c) {
				if (c.lowValue <= pair.value) mB[c.subID] = 1;
			});

			for_each(fData[0][att].begin(), fData[0][att].begin() + buck, [&](const vector<IntervalCombo> &bucketList) {
				const auto &&lowerBoundIterator = lower_bound(bucketList.begin(), bucketList.end(), pubPredicateTmp,
															  [&](const IntervalCombo &c1, const IntervalCombo &c2) {
																  return c1.highValue < c2.highValue;
															  });
				for_each(lowerBoundIterator, bucketList.end(), [&](const IntervalCombo &c) { mB[c.subID] = 1; });
			});
		} else { // Use high bucket list.
			const auto &&upperBoundIterator = upper_bound(fData[1][att][buck].begin(), fData[1][att][buck].end(),
														  pubPredicateTmp,
														  [&](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.lowValue < c2.lowValue;
														  });
			for_each(fData[1][att][buck].begin(), upperBoundIterator, [&](const IntervalCombo &c) {
				if (c.highValue >= pair.value) mB[c.subID] = 1;
			});

			for_each(fData[1][att].begin() + buck + 1, fData[1][att].end(),
					 [&](const vector<IntervalCombo> &bucketList) {
						 const auto &&upperBoundIterator = upper_bound(bucketList.begin(), bucketList.end(),
																	   pubPredicateTmp,
																	   [&](const IntervalCombo &c1,
																		   const IntervalCombo &c2) {
																		   return c1.lowValue < c2.lowValue;
																	   });
						 for_each(bucketList.begin(), upperBoundIterator,
								  [&](const IntervalCombo &c) { mB[c.subID] = 1; });
					 });
		}
		gB = gB & mB;
	}
	_for(i, 0, atts) if (!attExist[i])
			gB = gB & nB[i];
	matchSubs = gB.count();
}

bool Rein::deleteSubscription_forward_CBOMP(IntervalSub sub) { // δʵ��
	int find = 0;
	IntervalCnt cnt;
	int bucketID, id = sub.id;

	_for(i, 0, sub.size) {
		cnt = sub.constraints[i];

		bucketID = cnt.lowValue / buckStep;
		vector<Combo>::iterator it;
		for (it = data[0][cnt.att][bucketID].begin(); it != data[0][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[0][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}

		bucketID = cnt.highValue / buckStep;
		for (it = data[1][cnt.att][bucketID].begin(); it != data[1][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[1][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}
	}
	if (find == 2 * sub.size)
		numSub--;
	return find == 2 * sub.size;
}

int Rein::calMemory_forward_CBOMP() {
	long long size = sizeof(fData[0]) * 2; // Byte
	_for(i, 0, numDimension) {
		//cout <<i<<": "<< sizeof(data[0][i]) << ": ";
		size += sizeof(fData[0][i]) * 2;
		_for(j, 0, numBucket) {
			size += sizeof(fData[0][i][j]) + sizeof(fData[1][i][j]) +
					sizeof(IntervalCombo) * (fData[0][i][j].size() + fData[0][i][j].size());
			//cout << sizeof(data[0][i][j]) << " ";
		}
		//cout << "\n";
	}
	size += sizeof(subPredicate) + sizeof(counter);
	size += sizeof(bitset<subs>) * atts;
	size = size / 1024 / 1024; // MB
	return (int) size;
}


// ---------------------------------------------------------------------------------------------
// HybridRein (AWRein) 2022-01-08
void Rein::insert_hybrid_native(IntervalSub sub) {
	int bucketID;
	IntervalCombo ic;
	Combo cb;
	ic.subID = sub.id;
	cb.subID = sub.id;
	for (auto &&cnt: sub.constraints) {
		if (awRein_Ppoint <= cnt.highValue - cnt.lowValue) { // backward
			cb.val = cnt.lowValue;
			bucketID = cnt.lowValue / buckStep; // upper_bound probably is also OK!
			data[0][cnt.att][bucketID].insert(
				lower_bound(data[0][cnt.att][bucketID].begin(), data[0][cnt.att][bucketID].end(), cb,
							[&](const Combo &c1, const Combo &c2) {
								return c1.val < c2.val;
							}), cb);
			cb.val = cnt.highValue;
			bucketID = cnt.highValue / buckStep;
			data[1][cnt.att][bucketID].insert(
				lower_bound(data[1][cnt.att][bucketID].begin(), data[1][cnt.att][bucketID].end(), cb,
							[&](const Combo &c1, const Combo &c2) {
								return c1.val < c2.val;
							}), cb);
		} else { // forward
			subPredicate[sub.id]++;
			ic.lowValue = cnt.lowValue;
			ic.highValue = cnt.highValue;
			bucketID = cnt.lowValue / buckStep; // upper_bound probably is also OK!
			fData[0][cnt.att][bucketID].insert(
				lower_bound(fData[0][cnt.att][bucketID].begin(), fData[0][cnt.att][bucketID].end(), ic,
							[&](const IntervalCombo &c1, const IntervalCombo &c2) {
								return c1.highValue == c2.highValue ? c1.lowValue < c2.lowValue : c1.highValue <
																								  c2.highValue;
							}), ic);
			bucketID = cnt.highValue / buckStep;
			fData[1][cnt.att][bucketID].insert(
				lower_bound(fData[1][cnt.att][bucketID].begin(), fData[1][cnt.att][bucketID].end(), ic,
							[&](const IntervalCombo &c1, const IntervalCombo &c2) {
								return c1.lowValue == c2.lowValue ? c1.highValue < c2.highValue : c1.lowValue <
																								  c2.lowValue;
							}), ic);
		}
	}
	numSub++;
}

void Rein::match_hybrid_native(const Pub &pub, int &matchSubs) {
	memcpy(counter, subPredicate, sizeof(subPredicate));
	int att, buck, midBuck = buks >> 1;
	IntervalCombo pubPredicateTmp;
//	for (auto &&pair: pub.pairs) {
	for_each(pub.pairs.begin(), pub.pairs.end(), [&](const Pair &pair) {
		pubPredicateTmp.lowValue = pubPredicateTmp.highValue = pair.value, att = pair.att, buck = pair.value / buckStep;
		// cout<<"pubid= "<<pub.id<<" att= "<<att<<" value= "<<value<<endl;
		if (buck < midBuck) { // Use low bucket list.
			const auto &&lowerBoundIterator = lower_bound(fData[0][att][buck].begin(), fData[0][att][buck].end(),
														  pubPredicateTmp,
														  [](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.highValue < c2.highValue;
														  });
			for_each(lowerBoundIterator, fData[0][att][buck].end(), [&](const IntervalCombo &c) {
				if (c.lowValue <= pair.value) counter[c.subID]--;
			});

			for_each(fData[0][att].begin(), fData[0][att].begin() + buck, [&](const vector<IntervalCombo> &bucketList) {
				const auto &&lowerBoundIterator = lower_bound(bucketList.begin(), bucketList.end(), pubPredicateTmp,
															  [](const IntervalCombo &c1, const IntervalCombo &c2) {
																  return c1.highValue < c2.highValue;
															  });
				for_each(lowerBoundIterator, bucketList.end(), [&](const IntervalCombo &c) { counter[c.subID]--; });
			});
		} else { // Use high bucket list.
			const auto &&upperBoundIterator = upper_bound(fData[1][att][buck].begin(), fData[1][att][buck].end(),
														  pubPredicateTmp,
														  [&](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.lowValue < c2.lowValue;
														  });
			for_each(fData[1][att][buck].begin(), upperBoundIterator, [&](const IntervalCombo &c) {
				if (c.highValue >= pair.value) counter[c.subID]--;
			});

			for_each(fData[1][att].begin() + buck + 1, fData[1][att].end(),
					 [&](const vector<IntervalCombo> &bucketList) {
						 const auto &&upperBoundIterator = upper_bound(bucketList.begin(), bucketList.end(),
																	   pubPredicateTmp,
																	   [](const IntervalCombo &c1,
																		  const IntervalCombo &c2) {
																		   return c1.lowValue < c2.lowValue;
																	   });
						 for_each(bucketList.begin(), upperBoundIterator,
								  [&](const IntervalCombo &c) { counter[c.subID]--; });
					 });
		}
	});
//	}

	vector<bool> bits(numSub, false);
	vector<bool> attExist(numDimension, false);
	Combo cb;
//	for (auto &&pair: pub.pairs) {
	for_each(pub.pairs.begin(), pub.pairs.end(), [&](const Pair &pair) {
		att = pair.att, buck = pair.value / buckStep;
		attExist[att] = true;
		cb.val = pair.value;
		auto &&boundIterator = upper_bound(data[0][att][buck].begin(), data[0][att][buck].end(),
										   cb,
										   [](const Combo &c1, const Combo &c2) {
											   return c1.val < c2.val;
										   });
		for_each(boundIterator, data[0][att][buck].end(), [&bits](const Combo &c) {
			bits[c.subID] = true;
		});
		for_each(data[0][att].begin() + buck + 1, data[0][att].end(), [&bits](const vector<Combo> &bucketList) {
			for_each(bucketList.begin(), bucketList.end(), [&bits](const Combo &c) { bits[c.subID] = true; });
		});

		boundIterator = lower_bound(data[1][att][buck].begin(), data[1][att][buck].end(),
									cb,
									[](const Combo &c1, const Combo &c2) {
										return c1.val < c2.val;
									});
		for_each(data[1][att][buck].begin(), boundIterator, [&bits](const Combo &c) {
			bits[c.subID] = true;
		});
		for_each(data[1][att].begin(), data[1][att].begin() + buck, [&bits](const vector<Combo> &bucketList) {
			for_each(bucketList.begin(), bucketList.end(), [&bits](const Combo &c) { bits[c.subID] = true; });
		});

//		for (int k = 0; k < data[0][att][buck].size(); k++)
//			if (data[0][att][buck][k].val > pair.value)
//				bits[data[0][att][buck][k].subID] = true;
//		for (int k = 0; k < data[1][att][buck].size(); k++)
//			if (data[1][att][buck][k].val < pair.value)
//				bits[data[1][att][buck][k].subID] = true;
//
//		for (int j = buck + 1; j < numBucket; j++)
//			for (int k = 0; k < data[0][att][j].size(); k++)
//				bits[data[0][att][j][k].subID] = true;
//		for (int j = buck - 1; j >= 0; j--)
//			for (int k = 0; k < data[1][att][j].size(); k++)
//				bits[data[1][att][j][k].subID] = true;
	});
//	}

	for (int i = 0; i < numDimension; i++)
		if (!attExist[i])
			for (int j = 0; j < numBucket; j++)
				for (auto &&cb: data[0][i][j])
					bits[cb.subID] = true;

	for (int i = 0; i < subs; i++)
		if (!bits[i] && counter[i] == 0) {
			++matchSubs;
			//cout << "AWRein matches sub: " << i << endl;
		}
}

bool Rein::deleteSubscription_hybrid_native(IntervalSub sub) { // δʵ��
	int find = 0;
	IntervalCnt cnt;
	int bucketID, id = sub.id;

	_for(i, 0, sub.size) {
		cnt = sub.constraints[i];

		bucketID = cnt.lowValue / buckStep;
		vector<Combo>::iterator it;
		for (it = data[0][cnt.att][bucketID].begin(); it != data[0][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[0][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}

		bucketID = cnt.highValue / buckStep;
		for (it = data[1][cnt.att][bucketID].begin(); it != data[1][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[1][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}
	}
	if (find == 2 * sub.size)
		numSub--;
	return find == 2 * sub.size;
}

int Rein::calMemory_hybrid_native() {
	long long size = sizeof(fData[0]) * 2 + sizeof(data[0]) * 2; // Byte
	_for(i, 0, numDimension) {
		size += (sizeof(data[0][i]) + sizeof(fData[0][i])) * 2;
		_for(j, 0, numBucket) {
			size += (sizeof(data[0][i][j]) + sizeof(fData[0][i][j])) * 2 + \
            sizeof(Combo) * (data[0][i][j].size() + data[1][i][j].size()) + \
            sizeof(IntervalCombo) * (fData[0][i][j].size() + fData[0][i][j].size());
		}
	}
	size += sizeof(subPredicate) + sizeof(counter);
	size = size / 1024 / 1024; // MB
	return (int) size;
}



// ---------------------------------------------------------------------------------------------
// HybridRein with C-BOMP (AWRein_CBOMP) 2022-01-08

void Rein::insert_hybrid_CBOMP(IntervalSub sub) {
	int bucketID;
//	vector<bool> attrExist(atts, false);
	IntervalCombo ic;
	Combo cb;
	ic.subID = sub.id;
	cb.subID = sub.id;
	for (auto &&cnt: sub.constraints) {
		if (awRein_Ppoint <= cnt.highValue - cnt.lowValue) { // backward
			cb.val = cnt.lowValue;
			bucketID = cnt.lowValue / buckStep; // upper_bound probably is also OK!
			data[0][cnt.att][bucketID].insert(
				lower_bound(data[0][cnt.att][bucketID].begin(), data[0][cnt.att][bucketID].end(), cb,
							[&](const Combo &c1, const Combo &c2) {
								return c1.val < c2.val;
							}), cb);
			cb.val = cnt.highValue;
			bucketID = cnt.highValue / buckStep;
			data[1][cnt.att][bucketID].insert(
				lower_bound(data[1][cnt.att][bucketID].begin(), data[1][cnt.att][bucketID].end(), cb,
							[&](const Combo &c1, const Combo &c2) {
								return c1.val < c2.val;
							}), cb);
		} else { // forward
			nnB[cnt.att][sub.id] = true; // Only insert into forward data structure the forward matching think this attribute of the event is not null ! ('null' means match here)
//			attrExist[cnt.att] = true;
			ic.lowValue = cnt.lowValue;
			ic.highValue = cnt.highValue;
			bucketID = cnt.lowValue / buckStep; // upper_bound probably is also OK!
			fData[0][cnt.att][bucketID].insert(
				lower_bound(fData[0][cnt.att][bucketID].begin(), fData[0][cnt.att][bucketID].end(), ic,
							[&](const IntervalCombo &c1, const IntervalCombo &c2) {
								return c1.highValue == c2.highValue ? c1.lowValue < c2.lowValue : c1.highValue <
																								  c2.highValue;
							}), ic);
			bucketID = cnt.highValue / buckStep;
			fData[1][cnt.att][bucketID].insert(
				lower_bound(fData[1][cnt.att][bucketID].begin(), fData[1][cnt.att][bucketID].end(), ic,
							[&](const IntervalCombo &c1, const IntervalCombo &c2) {
								return c1.lowValue == c2.lowValue ? c1.highValue < c2.highValue : c1.lowValue <
																								  c2.lowValue;
							}), ic);
		}
	}
//	_for(i, 0, atts) if (!attrExist[i])
//			nB[i][sub.id] = 1;
	numSub++;
}

void Rein::match_hybrid_CBOMP(const Pub &pub, int &matchSubs) {
	int att, buck, midBuck = buks >> 1;
	vector<bool> attExist(atts, false);
	bitset<subs> backwardBits, mB;
	Combo cb;
//	for (auto &&pair: pub.pairs) {
	for_each(pub.pairs.begin(), pub.pairs.end(), [&](const Pair &pair) {
		att = pair.att, buck = pair.value / buckStep;
		attExist[att] = true;
		cb.val = pair.value;
		auto &&boundIterator = upper_bound(data[0][att][buck].begin(), data[0][att][buck].end(),
										   cb,
										   [](const Combo &c1, const Combo &c2) {
											   return c1.val < c2.val;
										   });
		for_each(boundIterator, data[0][att][buck].end(), [&backwardBits](const Combo &c) {
			backwardBits[c.subID] = 1;
		});
		for_each(data[0][att].begin() + buck + 1, data[0][att].end(), [&backwardBits](const vector<Combo> &bucketList) {
			for_each(bucketList.begin(), bucketList.end(),
					 [&backwardBits](const Combo &c) { backwardBits[c.subID] = 1; });
		});

		boundIterator = lower_bound(data[1][att][buck].begin(), data[1][att][buck].end(),
									cb,
									[](const Combo &c1, const Combo &c2) {
										return c1.val < c2.val;
									});
		for_each(data[1][att][buck].begin(), boundIterator, [&backwardBits](const Combo &c) {
			backwardBits[c.subID] = 1;
		});
		for_each(data[1][att].begin(), data[1][att].begin() + buck, [&backwardBits](const vector<Combo> &bucketList) {
			for_each(bucketList.begin(), bucketList.end(),
					 [&backwardBits](const Combo &c) { backwardBits[c.subID] = 1; });
		});
//		for (int k = 0; k < data[0][att][buck].size(); k++)
//			if (data[0][att][buck][k].val > pair.value)
//				backwardBits[data[0][att][buck][k].subID] = 1;
//		for (int k = 0; k < data[1][att][buck].size(); k++)
//			if (data[1][att][buck][k].val < pair.value)
//				backwardBits[data[1][att][buck][k].subID] = 1;
//
//		for (int j = buck + 1; j < numBucket; j++)
//			for (int k = 0; k < data[0][att][j].size(); k++)
//				backwardBits[data[0][att][j][k].subID] = 1;
//		for (int j = buck - 1; j >= 0; j--)
//			for (int k = 0; k < data[1][att][j].size(); k++)
//				backwardBits[data[1][att][j][k].subID] = 1;
	});

	IntervalCombo pubPredicateTmp;
//	for (auto &&pair: pub.pairs) {
	for_each(pub.pairs.begin(), pub.pairs.end(), [&](const Pair &pair) {
		pubPredicateTmp.lowValue = pubPredicateTmp.highValue = pair.value, att = pair.att, buck = pair.value / buckStep;
		mB = nnB[att];
		if (buck < midBuck) { // Use low bucket list.
			const auto &&lowerBoundIterator = lower_bound(fData[0][att][buck].begin(), fData[0][att][buck].end(),
														  pubPredicateTmp,
														  [](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.highValue < c2.highValue;
														  });
			for_each(lowerBoundIterator, fData[0][att][buck].end(), [&mB, &pair](const IntervalCombo &c) {
				if (c.lowValue <= pair.value) mB[c.subID] = 0;
			});

			for_each(fData[0][att].begin(), fData[0][att].begin() + buck, [&](const vector<IntervalCombo> &bucketList) {
				auto &&lowerBoundIterator = lower_bound(bucketList.begin(), bucketList.end(), pubPredicateTmp,
														[](const IntervalCombo &c1, const IntervalCombo &c2) {
															return c1.highValue < c2.highValue;
														});
				for_each(lowerBoundIterator, bucketList.end(), [&mB](const IntervalCombo &c) { mB[c.subID] = 0; });
			});
		} else { // Use high bucket list.
			const auto &&upperBoundIterator = upper_bound(fData[1][att][buck].begin(), fData[1][att][buck].end(),
														  pubPredicateTmp,
														  [](const IntervalCombo &c1, const IntervalCombo &c2) {
															  return c1.lowValue < c2.lowValue;
														  });
			for_each(fData[1][att][buck].begin(), upperBoundIterator, [&mB, &pair](const IntervalCombo &c) {
				if (c.highValue >= pair.value) mB[c.subID] = 0;
			});

			for_each(fData[1][att].begin() + buck + 1, fData[1][att].end(), [&](const vector<IntervalCombo> &bucketList) {
				auto &&upperBoundIterator = upper_bound(bucketList.begin(), bucketList.end(), pubPredicateTmp,
														[](const IntervalCombo &c1, const IntervalCombo &c2) {
															return c1.lowValue < c2.lowValue;
														});
				for_each(bucketList.begin(), upperBoundIterator, [&mB](const IntervalCombo &c) { mB[c.subID] = 0; });
			});
		}
		backwardBits = backwardBits | mB;
	});

	_for(i, 0, atts) if (!attExist[i]) {
			for (int j = 0; j < numBucket; j++)
				for (auto &&cb: data[0][i][j])
					backwardBits[cb.subID] = 1;
			backwardBits = backwardBits | nnB[i];
		}
	matchSubs = subs - backwardBits.count();
}

bool Rein::deleteSubscription_hybrid_CBOMP(IntervalSub sub) { // δʵ��
	int find = 0;
	IntervalCnt cnt;
	int bucketID, id = sub.id;

	_for(i, 0, sub.size) {
		cnt = sub.constraints[i];

		bucketID = cnt.lowValue / buckStep;
		vector<Combo>::iterator it;
		for (it = data[0][cnt.att][bucketID].begin(); it != data[0][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[0][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}

		bucketID = cnt.highValue / buckStep;
		for (it = data[1][cnt.att][bucketID].begin(); it != data[1][cnt.att][bucketID].end(); it++)
			if (it->subID == id) {
				data[1][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}
	}
	if (find == 2 * sub.size)
		numSub--;
	return find == 2 * sub.size;
}

int Rein::calMemory_hybrid_CBOMP() {
	long long size = sizeof(fData[0]) * 2 + sizeof(data[0]) * 2; // Byte
	_for(i, 0, numDimension) {
		size += (sizeof(data[0][i]) + sizeof(fData[0][i])) * 2;
		_for(j, 0, numBucket) {
			size += (sizeof(data[0][i][j]) + sizeof(fData[0][i][j])) * 2 + \
            sizeof(Combo) * (data[0][i][j].size() + data[1][i][j].size()) + \
            sizeof(IntervalCombo) * (fData[0][i][j].size() + fData[0][i][j].size());
		}
	}
	size += sizeof(bitset<subs>) * atts; // nnB
	size = size / 1024 / 1024; // MB
	return (int) size;
}



// Measurement/Visualization function

void Rein::calBucketSize() {
	bucketSub.clear();
	bucketSub.resize(numBucket);
	_for(i, 0, numDimension) _for(j, 0, numBucket) {
			_for(k, 0, data[0][i][j].size()) bucketSub[j].insert(data[0][i][j][k].subID);
			_for(k, 0, data[1][i][j].size()) bucketSub[j].insert(data[1][i][j][k].subID);
		}
}

vector<int> Rein::calMarkNumForBuckets() {
	vector<int> numMarking(numBucket, 0);
	_for(i, 0, numBucket) {
		_for(j, 0, numDimension) {
			_for(k, i, numBucket) {
				numMarking[i] += data[0][j][k].size();
			}
			_for(k, 0, i + 1) {
				numMarking[i] += data[1][j][k].size();
			}
		}
	}
	return numMarking;
}

//kafka


void Rein::run_Rein_Original_consumer(const intervalGenerator &gen)
{

	Rein rein(OriginalRein);

	vector<double> insertTimeList;
	vector<double> deleteTimeList;
	vector<double> matchTimeList_rein;
	vector<vector<double>> matchSubList_rein;
	vector<double>aveAdareinWindowLatencySum;
	vector<double>aveReinWindowLatencySum;
	vector<double> falsePositiveRateVec;
	// insert
	for (int i = 0; i < subs; i++)
	{
		// Timer insertStart;

		//insert_sss_c_w(gen.subList[i]); // Insert sub[i] into data structure.
		rein.insert_backward_original(gen.subList[i]);

		// int64_t insertTime = insertStart.elapsed_nano(); // Record inserting time in nanosecond.
		// insertTimeList.push_back((double)insertTime / 1000000);
	}
	cout << "Insertion Finishes.\n";

	double initTime;
	Timer initStart;
	//static_succession_selection_crossed_width(falsePositiveRate, gen.subList);
	initTime = (double)initStart.elapsed_nano() / 1000000.0; //输出微秒
	cout << "AdaRein_SSS_C_W Skipping Task Finishes.\n";

	double lastWindowFalsePositiveRate = falsePositiveRate;
	double lastWindowLatencySum = 0.0;

	double reinWindowLatencySum = 0.0;

	int windowEventNum = 0;
	int matchSubs;
	double matchTime;

	queue<string> pubQueue;
	Pub pub;

	int windowNo = 1;

	// initial consumer
	std::string brokers = "localhost:9092";
	std::string errstr = "";
	std::string topic_str = "newAdRein";
	MyHashPartitionerCb hash_partitioner;
	int32_t partition = RdKafka::Topic::PARTITION_UA; //Ϊ�β����ã�����Consumer����ֻ��д0�������޷��Զ��𣿣���
	partition = 0;
	int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
	bool do_conf_dump = false;
	int opt;

	// Create configuration objects
	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	if (tconf->set("partitioner_cb", &hash_partitioner, errstr) != RdKafka::Conf::CONF_OK)
	{
		std::cerr << errstr << std::endl;
		exit(1);
	}

	/* * Set configuration properties */
	conf->set("metadata.broker.list", brokers, errstr);
	ExampleEventCb ex_event_cb;
	conf->set("event_cb", &ex_event_cb, errstr);

	ExampleDeliveryReportCb ex_dr_cb;

	/* Set delivery report callback */
	conf->set("dr_cb", &ex_dr_cb, errstr);
	/* * Create consumer using accumulated global configuration. */
	RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
	if (!consumer)
	{
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		exit(1);
	}

	std::cout << "% Created consumer " << consumer->name() << std::endl;

	/* * Create topic handle. */
	RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topic_str, tconf, errstr);
	if (!topic)
	{
		std::cerr << "Failed to create topic: " << errstr << std::endl;
		exit(1);
	}

	/* * Start consumer for topic+partition at start offset */
	RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
	if (resp != RdKafka::ERR_NO_ERROR)
	{
		std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp) << std::endl;
		exit(1);
	}

	ExampleConsumeCb ex_consume_cb;
	//output to file
    ofstream file("/home/k8smaster/dypKafka01/dypKafka/HEM/Rein_kafka.txt");
	while (true)
	{
		reinWindowLatencySum = 0.0;
		windowEventNum = 0;
		matchSubList_rein.push_back({});

		while (windowEventNum < windowSize)
		{
			std::string msg_pubstr;
			double timestamp;
			if (run)
			{
				RdKafka::Message *msg;
				do
				{
					msg = consumer->consume(topic, partition, 1000000);
					msg_pubstr = msg_consume(msg, NULL);
				} while (msg_pubstr == "errors");
				struct timespec tn;
				clock_gettime(0, &tn);
				double now = (tn.tv_sec*1000000000+tn.tv_nsec)/1000000;
				timestamp = (now - (double)msg->timestamp().timestamp); //msg->timestamp(): millsecond
				cout << "now: "<< (unsigned long long)now << ", tstamp: " << msg->timestamp().timestamp << ", diff: " << timestamp << "\n";

				delete msg;
				consumer->poll(0);
			}
			else
			{
				std::cout << "run == false" << endl;
				exit(0);
			}

			istringstream iss(msg_pubstr); // string sentence;
			string t;

			int tmp_attr = 0;
			pub.pairs.resize(0);
			while (getline(iss, t, ' '))
			{
				pub.pairs.push_back({tmp_attr++, stoi(t)});
			}
			pub.size = tmp_attr;
			// cout << "receive event values: " << pub.pairs.size() << "\n";
			//reinWindowLatencySum += timestamp;
            
			clock_t start_time, end_time;
			Timer matchStart;
			matchSubs=1;
			rein.match_backward_original(pub, matchSubs);
			matchTime = (double)matchStart.elapsed_nano() / 1000000;
			reinWindowLatencySum += timestamp+matchTime;
			matchSubList_rein[matchSubList_rein.size() - 1].push_back(matchSubs);
			matchTimeList_rein.push_back(timestamp + matchTime);
			//  cout << "rein match time = " << matchTime << "ms, matchSubs= " << matchSubs << endl;
			if (file.is_open())
			{
				file << "WindowNo " << windowNo << ", rein match time = " << matchTime << "ms, matchSubs= " << matchSubs << "; " << endl;
				//file.close();
			}
			windowEventNum++;
		} // a window
      
		/* adareinWindowLatencySum = adareinWindowLatencySum / windowSize;
		//aveAdareinWindowLatencySum.push_back(adareinWindowLatencySum);
		reinWindowLatencySum = reinWindowLatencySum / windowSize;
        //aveReinWindowLatencySum.push_back(reinWindowLatencySum);

		// [4,5]
		double nextWindowFPR = lastWindowFalsePositiveRate;
		if (adareinWindowLatencySum >= max(waitTimeUpperThreshold, lastWindowLatencySum)) // 当前延时大于阈值上限且大于上一个时间窗延时
		{
			nextWindowFPR = min(lastWindowFalsePositiveRate + falsePositiveRateMaxVariation, falsePositiveRateMax);
		}
		else if (adareinWindowLatencySum < min(waitTimeLowerThreshold, lastWindowLatencySum)) // 当前延时低于阈值下限且低于上一个时间窗的延时
		{
			nextWindowFPR = max(lastWindowFalsePositiveRate - falsePositiveRateMaxVariation, 0.0);
		}
		if (nextWindowFPR != lastWindowFalsePositiveRate)
			static_succession_selection_crossed_width(nextWindowFPR, gen.subList);

		cout << "windowNo: " << windowNo << ": lastWinLat= " << lastWindowLatencySum
			 << ", avgAdaReinWindowLatency= " << adareinWindowLatencySum
			 << ", avgReinWindowLatency= " << reinWindowLatencySum << "\n"
			 << "lastWindowFalsePositiveRate= " << lastWindowFalsePositiveRate
			 << ", nextWindowFPR= " << nextWindowFPR << "\n";

		lastWindowFalsePositiveRate = nextWindowFPR;
		lastWindowLatencySum = adareinWindowLatencySum;  */
		windowNo++;
		if (windowNo == 35)
			break; 

	}

	/*consumer->stop(topic, partition);
	consumer->poll(0);
	delete topic;
	delete consumer;

	double falseAvgMatchNum = Util::Average(matchSubList_ad2); */

	// #ifdef DEBUG
	// 	cout << "falseMatchNum= " << falseAvgMatchNum << ", realFalsePositiveRate= "
	// 		 << 1 - realMatchNum / falseAvgMatchNum << ", matchTime= "
	// 		 << Util::Double2String(Util::Average(matchTimeList)) << " ms\n";
	// #endif
    //没有输出，延迟，论文里面画的是 second-eventLatency 图
	/* // output
	string outputFileName = "AdaRein_SSS_C_W_kafka.txt";
	string content = expID + " memory= " + Util::Int2String(calMemory_sss_c_w()) + " MB AvgMatchNum= " + Util::Double2String(Util::Average(matchSubList_ad2)) + " AvgInsertTime= " + Util::Double2String(Util::Average(insertTimeList)) + " ms InitTime= " + Util::Double2String(initTime) + " ms AvgConstructionTime= " +
					 Util::Double2String(Util::Average(insertTimeList) + initTime / subs) + " ms AvgDeleteTime= " + Util::Double2String(Util::Average(deleteTimeList)) + " ms AvgMatchTime= " + Util::Double2String(Util::Average(matchTimeList_ad2)) + " ms level= " + Util::Int2String(adarein_level) + " maxSkipPre= " + Util::Int2String(maxSkipPredicate) + " fPR= " + Util::Double2String(falsePositiveRate) + " realfPR= " + Util::Double2String(1 - realMatchNum / falseAvgMatchNum) + " numSub= " + Util::Int2String(subs) + " subSize= " + Util::Int2String(cons) + " numPub= " + Util::Int2String(pubs) + " pubSize= " + Util::Int2String(m) + " attTypes= " + Util::Int2String(atts) + " attGroup= " + Util::Int2String(attrGroup) + " attNumType= " + Util::Int2String(attNumType) + " valDom= " + Util::Double2String(valDom);
	content.append("\nMatchTimePerEvent= [");
	for(int i=0;i<matchTimeList_ad2.size()-1;i++){
		content.append(to_string(matchTimeList_ad2[i])).append(", ");
	}
	content.append(to_string(matchTimeList_ad2.back())).append("]\n\n");
	Util::WriteData2Begin(outputFileName.c_str(), content);

	outputFileName = "tmpData/AdaRein_SSS_C_W_kafka.txt";
	content = Util::Double2String(Util::Average(matchTimeList_ad2)) + ", ";
	Util::WriteData2End(outputFileName.c_str(), content); */


	string outputFileName = "Rein_kafka.txt";
	string content = expID + " memory= " + Util::Int2String(rein.calMemory_backward_original()) + " MB, AvgInsertTime= " + Util::Double2String(Util::Average(insertTimeList)) + " ms InitTime= " + Util::Double2String(initTime) + " ms AvgConstructionTime= " +
			  Util::Double2String(Util::Average(insertTimeList) + initTime / subs) + " ms AvgDeleteTime= " + Util::Double2String(Util::Average(deleteTimeList)) + " ms AvgMatchTime= " + Util::Double2String(Util::Average(matchTimeList_rein)) +  " subSize= " + Util::Int2String(cons) + " numPub= " + Util::Int2String(pubs) + " pubSize= " + Util::Int2String(m) + " attTypes= " + Util::Int2String(atts) + " attGroup= " + Util::Int2String(attrGroup) + " attNumType= " + Util::Int2String(attNumType) + " valDom= " + Util::Double2String(valDom);

	content.append("\nMatchSubPerEvent= [\n");
	for (int i = 0; i < matchSubList_rein.size() - 1; i++)
	{
		content.append("[");
		for (int j = 0; j < windowSize; j++)
		{
			content.append(to_string(matchSubList_rein[i][j]));
			if (j < windowSize - 1)
				content.append(", ");
		}
		content.append("]");
		if (i < matchSubList_rein.size() - 1)
			content.append(",\n");
		else
			content.append("\n]\n\n");
	}
	
	content.append("\nMatchTimePerEvent= [");
	for(int i=0;i<matchTimeList_rein.size()-1;i++){
		content.append(to_string(matchTimeList_rein[i])).append(", ");
	}
	content.append(to_string(matchTimeList_rein.back())).append("]\n\n");
	Util::WriteData2Begin(outputFileName.c_str(), content);


	outputFileName = "tmpData/Rein_kafka.txt";
	content = Util::Double2String(Util::Average(matchTimeList_rein)) + ", ";
	Util::WriteData2End(outputFileName.c_str(), content);
}
