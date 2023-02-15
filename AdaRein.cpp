#include "AdaRein.h"
// #include "librdkafka/rdkafkacpp.h"
// #include <time.h>
// #include <csignal>
AdaRein::AdaRein(int type) : numSub(0)
{
	buckStep = (valDom - 1) / buks + 1;
	numBucket = (valDom - 1) / buckStep + 1;

	string TYPE;
	switch (type)
	{
	case AdaRein_ORI:
		TYPE = "AdaRein_ORI";
		attsCounts.resize(atts);
		data[0].resize(atts, vector<vector<Combo>>(numBucket));
		data[1].resize(atts, vector<vector<Combo>>(numBucket));
		skipped.resize(atts, false);
		break;
	case AdaRein_SSS:
		TYPE = "AdaRein_SSS";
		attsCounts.resize(atts);
		endBucket[0].resize(atts, buks - 1);
		endBucket[1].resize(atts, 0);
		data[0].resize(atts, vector<vector<Combo>>(numBucket));
		data[1].resize(atts, vector<vector<Combo>>(numBucket));
		skipped.resize(atts, false);
		break;
	case AdaRein_SSS_B:
		TYPE = "AdaRein_SSS_B";
		attsCounts.resize(atts);
		beginBucket[0].resize(atts, 0);
		beginBucket[1].resize(atts, buks - 1);
		data[0].resize(atts, vector<vector<Combo>>(numBucket));
		data[1].resize(atts, vector<vector<Combo>>(numBucket));
		skipped.resize(atts, false);
		break;
	case AdaRein_SSS_C:
		TYPE = "AdaRein_SSS_C";
		attsCounts.resize(atts);
		beginBucket[0].resize(atts, 0);
		beginBucket[1].resize(atts, buks - 1);
		endBucket[0].resize(atts, buks - 1);
		endBucket[1].resize(atts, 0);
		data[0].resize(atts, vector<vector<Combo>>(numBucket));
		data[1].resize(atts, vector<vector<Combo>>(numBucket));
		skipped.resize(atts, false);
		break;
	case AdaRein_SSS_C_PPH: // δʵ��
		TYPE = "AdaRein_SSS_C_PPH";
		buckStep = (valDom - 1) / (buks / 2) + 1;
		// dividerValue = divider * valDom;
		attsCounts.resize(atts);
		beginBucket[0].resize(atts, 0);
		beginBucket[1].resize(atts, buks - 1);
		endBucket[0].resize(atts, buks - 1);
		endBucket[1].resize(atts, 0);
		/*_for(i, 0, adarein_level) {
			dataL[i][0].resize(atts, vector<vector<Combo >>(numBucket*));
			dataL[i][1].resize(atts, vector<vector<Combo >>(numBucket*()));
		}*/
		skipped.resize(atts, false);
		break;
	case AdaRein_SSS_C_W:
		TYPE = "AdaRein_SSS_C_W" + to_string(adarein_level);
		levelBuks = buks / adarein_level;			  // ÿһ���Ͱ��
		levelBuckStep = (valDom - 1) / levelBuks + 1; // 3 hour-bug: buckStep = (valDom - 1) / buks + 1         ÿһ���ÿ��Ͱ�������������
		levelBuks = (valDom - 1) / levelBuckStep + 1; // bug: buckStep-levelBuckStep
		widthStep = (valDom - 1) / adarein_level + 1; // ���ڲ�֮��ν�������ȵĲ�
		attsCounts.resize(atts);
		skippedW.resize(atts, vector<bool>(adarein_level, false));
		dataW.resize(atts, vector<vector<vector<vector<Combo>>>>(
							   adarein_level, vector<vector<vector<Combo>>>(
												  2, vector<vector<Combo>>(levelBuks))));
		beBucketW.resize(atts, vector<pair<pair<int, int>, pair<int, int>>>(adarein_level));
		_for(i, 0, atts)
		{
			_for(j, 0, adarein_level)
			{
				beBucketW[i][j].first.first = 1;			  // low begin
				beBucketW[i][j].first.second = levelBuks - 1; // low end
				beBucketW[i][j].second.first = levelBuks - 2; // high begin
				beBucketW[i][j].second.second = 0;			  // high end
			}
		}
		break;
	case pAdaRein_SSS_C_W:
		TYPE = "pAdaRein_SSS_C_W" + to_string(adarein_level);
		levelBuks = buks / adarein_level;
		levelBuckStep = (valDom - 1) / levelBuks + 1; // 3 hour-bug: buckStep =
		levelBuks = (valDom - 1) / levelBuckStep + 1; // bug: buckStep-levelBuckStep
		widthStep = (valDom - 1) / adarein_level + 1;
		attsCounts.resize(atts);
		skippedW.resize(atts, vector<bool>(adarein_level, false));
		dataW.resize(atts, vector<vector<vector<vector<Combo>>>>(
							   adarein_level, vector<vector<vector<Combo>>>(
												  2, vector<vector<Combo>>(levelBuks))));
		beBucketW.resize(atts, vector<pair<pair<int, int>, pair<int, int>>>(adarein_level));
		_for(i, 0, atts)
		{
			_for(j, 0, adarein_level)
			{
				beBucketW[i][j].first.first = 1;			  // low begin
				beBucketW[i][j].first.second = levelBuks - 1; // low end
				beBucketW[i][j].second.first = levelBuks - 2; // high begin
				beBucketW[i][j].second.second = 0;			  // high end
			}
		}
		threadPool.initThreadPool(parallelDegree);
		break;
	case p2AdaRein_SSS_C_W:
		TYPE = "p2AdaRein_SSS_C_W" + to_string(adarein_level);
		levelBuks = buks / adarein_level;
		levelBuckStep = (valDom - 1) / levelBuks + 1; // 3 hour-bug: buckStep =
		levelBuks = (valDom - 1) / levelBuckStep + 1; // bug: buckStep-levelBuckStep
		widthStep = (valDom - 1) / adarein_level + 1;
		attsCounts.resize(atts); // will be replaced by attsWidthPredicate
		attsWidthPredicate.resize(atts, vector<int>(adarein_level));
		skippedW.resize(atts, vector<bool>(adarein_level, false));
		dataW.resize(atts, vector<vector<vector<vector<Combo>>>>(
							   adarein_level, vector<vector<vector<Combo>>>(
												  2, vector<vector<Combo>>(levelBuks))));
		beBucketW.resize(atts, vector<pair<pair<int, int>, pair<int, int>>>(adarein_level));
		_for(i, 0, atts)
		{
			_for(j, 0, adarein_level)
			{
				beBucketW[i][j].first.first = 1;			  // low begin
				beBucketW[i][j].first.second = levelBuks - 1; // low end
				beBucketW[i][j].second.first = levelBuks - 2; // high begin
				beBucketW[i][j].second.second = 0;			  // high end
			}
		}
		threadPool.initThreadPool(parallelDegree);
		threadTaskSet.resize(parallelDegree, vector<pair<int, int>>());
		break;
	case AdaRein_SDS:
		TYPE = "AdaRein_SDS";
		break;
	case AdaRein_DSS_W:
		TYPE = "AdaRein_DSS_W" + to_string(adarein_level);
		levelBuks = buks / adarein_level;
		levelBuckStep = (valDom - 1) / levelBuks + 1;
		levelBuks = (valDom - 1) / levelBuckStep + 1;
		widthStep = (valDom - 1) / adarein_level + 1;
		attsWidthPredicate.resize(atts, vector<int>(adarein_level));
		numSkipPredicateInTotal = numSkipBuckInTotal = numSkipAttsInTotal = 0;
		dataW.resize(atts, vector<vector<vector<vector<Combo>>>>(
							   adarein_level, vector<vector<vector<Combo>>>(
												  2, vector<vector<Combo>>(levelBuks))));
		break;
	case AdaRein_DSS_B:
		TYPE = "AdaRein_DSS_B";
		data[0].resize(atts, vector<vector<Combo>>(numBucket));
		data[1].resize(atts, vector<vector<Combo>>(numBucket));
		attsPredicate.resize(atts, 0);
		numSkipPredicateInTotal = numSkipBuckInTotal = numSkipAttsInTotal = 0;
		break;
	case AdaRein_DSS_B_W:
		TYPE = "AdaRein_DSS_B_W" + to_string(adarein_level);
		levelBuks = buks / adarein_level;
		levelBuckStep = (valDom - 1) / levelBuks + 1;
		levelBuks = (valDom - 1) / levelBuckStep + 1;
		widthStep = (valDom - 1) / adarein_level + 1;
		attsWidthPredicate.resize(atts, vector<int>(adarein_level));
		numSkipPredicateInTotal = numSkipBuckInTotal = numSkipAttsInTotal = 0;
		dataW.resize(atts, vector<vector<vector<vector<Combo>>>>(
							   adarein_level, vector<vector<vector<Combo>>>(
												  2, vector<vector<Combo>>(levelBuks))));
		break;
	case AdaRein_DDS_B:
		TYPE = "AdaRein_DDS_B";
		break;
	case AdaRein_IBU:
		TYPE = "AdaRein_IBU";
		break;
	default:
		break;
	}
	cout << "ExpID = " << expID << ". " + TYPE + ": falsePositiveRate_local = " << falsePositiveRate
		 << ", bucketStep = "
		 << buckStep << ", numBucket = " << numBucket;
	if (type == AdaRein_SSS_C_PPH || type == AdaRein_SSS_C_W)
		cout << ", adarein_level" << adarein_level;
	cout << endl;
}

void AdaRein::insert(IntervalSub sub)
{
	for (int i = 0; i < sub.size; i++)
	{
		IntervalCnt cnt = sub.constraints[i];
		Combo c;
		c.val = cnt.lowValue;
		c.subID = sub.id;
		data[0][cnt.att][c.val / buckStep].push_back(c);
		c.val = cnt.highValue;
		data[1][cnt.att][c.val / buckStep].push_back(c);
	}
	numSub++;
}

bool AdaRein::deleteSubscription(IntervalSub sub)
{
	int find = 0;
	IntervalCnt cnt;
	int bucketID, id = sub.id;

	_for(i, 0, sub.size)
	{
		cnt = sub.constraints[i];
		bucketID = cnt.lowValue / buckStep;
		vector<Combo>::iterator it;
		for (it = data[0][cnt.att][bucketID].begin(); it != data[0][cnt.att][bucketID].end(); it++)
			if (it->subID == id)
			{
				data[0][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}

		bucketID = cnt.highValue / buckStep;
		for (it = data[1][cnt.att][bucketID].begin(); it != data[1][cnt.att][bucketID].end(); it++)
			if (it->subID == id)
			{
				data[1][cnt.att][bucketID].erase(it); // it =
				find++;
				break;
			}
	}
	if (find == 2 * sub.size)
		numSub--;
	return find == 2 * sub.size;
}

void AdaRein::exact_match(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	vector<bool> bits(subs, false);
	vector<bool> attExist(atts, false);

	for (int i = 0; i < pub.size; i++)
	{
		int value = pub.pairs[i].value, att = pub.pairs[i].att, buck = value / buckStep;
		attExist[att] = true;
		for (int k = 0; k < data[0][att][buck].size(); k++)
			if (data[0][att][buck][k].val > value)
				bits[data[0][att][buck][k].subID] = true;
		for (int j = buck + 1; j < numBucket; j++)
			for (int k = 0; k < data[0][att][j].size(); k++)
				bits[data[0][att][j][k].subID] = true;

		for (int k = 0; k < data[1][att][buck].size(); k++)
			if (data[1][att][buck][k].val < value)
				bits[data[1][att][buck][k].subID] = true;
		for (int j = buck - 1; j >= 0; j--)
			for (int k = 0; k < data[1][att][j].size(); k++)
				bits[data[1][att][j][k].subID] = true;
	}

	for (int i = 0; i < atts; i++)
		if (!attExist[i])
			for (int j = 0; j < numBucket; j++)
				for (int k = 0; k < data[0][i][j].size(); k++)
					bits[data[0][i][j][k].subID] = true;

	for (int i = 0; i < subList.size(); i++)
		if (!bits[i])
			++matchSubs;
}

void AdaRein::original_selection(double falsePositive, const vector<IntervalSub> &subList)
{
	for (int i = 0; i < atts; i++)
	{
		attsCounts[i].att = i;
		attsCounts[i].count = 0;
	}

	int numPredicate = 0, numSkipPredicate = 0;
	double avgSubSize = 0, avgWidth = 0;

	for (int i = 0; i < subList.size(); i++)
	{
		numPredicate += subList[i].size;
		for (int j = 0; j < subList[i].size; j++)
		{
			++attsCounts[subList[i].constraints[j].att].count;
			avgWidth += subList[i].constraints[j].highValue - subList[i].constraints[j].lowValue;
		}
	}
	avgSubSize = (double)numPredicate / subList.size();
	avgWidth /= (double)numPredicate * valDom;

	// faster version, need to be verified.--û����avgWidth��
	/*_for(skipIndex, 0, atts) {
		_for(j, 0, bucks)
			attsCounts[skipIndex].count += data[0][skipIndex][j].size();
		numPredicate += attsCounts[skipIndex].count;
	}*/

	sort(attsCounts.begin(), attsCounts.end());

	maxSkipPredicate =
		numPredicate - (avgSubSize + log(falsePositive + 1) / log(avgWidth)) *
						   subs; // ������Թ��˵�ν����, currentSum�����ֵ
	cout << "maxSkipPredicate= " << maxSkipPredicate << "\n";

#ifdef DEBUG
	int numSkipAttr = 0;
#endif // DEBUG

	for (int i = 0; i < atts; i++)
	{
		numSkipPredicate += attsCounts[i].count;
		// ��Ч�汾:
		if ((double)(numPredicate - numSkipPredicate) / (double)subs >
			avgSubSize + log(falsePositive + 1) / log(avgWidth))
		{
			//		if ((double)(numPredicate - numSkipPredicate) / (double)subList.size() > subList[0].constraints.size() + log(falsePositive + 1) / log((subList[0].constraints[0].highValue - subList[0].constraints[0].lowValue) / (double)valDom)) {
			skipped[attsCounts[i].att] = true;
#ifdef DEBUG
			numSkipAttr++;
			// cout << "Skip Attribute " << attsCounts[skipIndex].att<<"\n"; // could output in finding order.
#endif
		}
		else
		{
			numSkipPredicate -= attsCounts[i].count; // back
			break;
		}
	}

#ifdef DEBUG
	cout << "avgSubSize= " << avgSubSize << ", "
		 << "avgWidth= " << avgWidth << ", numPredicate= " << numPredicate
		 << ", maxSkipPredicate= " << maxSkipPredicate << ", numSkipPredicate= " << numSkipPredicate << ".\n";
	cout << "Total skipped attribute: " << numSkipAttr << " among " << atts << " attributes.\n";
	cout << "Skip attribute:";
	_for(i, 0, atts) if (skipped[i])
			cout
		<< " " << i;
	cout << "\n";
#endif
}

void AdaRein::approx_match_ori(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	vector<bool> bits(subs, false);
	vector<bool> attExist(atts, false);
	for (int i = 0; i < pub.size; i++)
	{
		int att = pub.pairs[i].att;
		attExist[att] = true;
		if (skipped[att])
			continue;
		int value = pub.pairs[i].value, buck = value / buckStep;
		for (int k = 0; k < data[0][att][buck].size(); k++)
			if (data[0][att][buck][k].val > value)
				bits[data[0][att][buck][k].subID] = true;
		for (int j = buck + 1; j < numBucket; j++)
			for (int k = 0; k < data[0][att][j].size(); k++)
				bits[data[0][att][j][k].subID] = true;

		for (int k = 0; k < data[1][att][buck].size(); k++)
			if (data[1][att][buck][k].val < value)
				bits[data[1][att][buck][k].subID] = true;
		for (int j = buck - 1; j >= 0; j--)
			for (int k = 0; k < data[1][att][j].size(); k++)
				bits[data[1][att][j][k].subID] = true;
	}

	for (int i = 0; i < atts; i++)
		if (!attExist[i] && !skipped[i])
			for (int j = 0; j < numBucket; j++)
				for (int k = 0; k < data[0][i][j].size(); k++)
					bits[data[0][i][j][k].subID] = true;

	for (int i = 0; i < subList.size(); i++)
		if (!bits[i])
			++matchSubs;
}

void AdaRein::static_succession_selection(double falsePositive, const vector<IntervalSub> &subList)
{
	for (int i = 0; i < atts; i++)
	{
		attsCounts[i].att = i;
		attsCounts[i].count = 0;
	}

	int numPredicate = 0, numSkipPredicate = 0; // ν������, �ѹ��˵�ν������
	double avgSubSize = 0, avgWidth = 0;		// ƽ��ÿ�������ж��ٸ�ν��, ν�ʵ�ƽ������
	for (auto &&iSub : subList)
	{
		// numPredicate += iSub.constraints.size(); // ����
		for (auto &&iCnt : iSub.constraints)
		{
			//++attsCounts[iCnt.att].count; // ����
			avgWidth += iCnt.highValue - iCnt.lowValue;
		}
	}
	// ��Ҫ�Ȳ��붩�Ĳ�����ô��
	_for(i, 0, atts)
	{
		_for(j, 0, buks) attsCounts[i].count += data[0][i][j].size();
		numPredicate += attsCounts[i].count;
	}
	avgSubSize = (double)numPredicate / subList.size();
	avgWidth /= (double)numPredicate * valDom;

	sort(attsCounts.begin(), attsCounts.end());

	// minPredicate �����Դ���
	// double minK = log(pow(avgWidth, avgSubSize) + falsePositive) / log(avgWidth);
	// inline auto valid = [&](double) {return (double)(numPredicate - numSkipPredicate) > minK * subs; };

	double falsePositiveRate_global = pow(avgWidth, avgSubSize) * subs / (1 - falsePositive) * falsePositive / subs;

#ifdef DEBUG
	maxSkipPredicate = numPredicate - (avgSubSize - log(1 - falsePositive) / log(avgWidth)) * subs; // k2
	cout << "k2= " << maxSkipPredicate << "\n";
	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) + falsePositiveRate_global) / log(avgWidth) *
						   subs; // ������Թ��˵�ν����, currentSum�����ֵ
	cout << "k3_global= " << maxSkipPredicate << "\n";
	int numSkipAttr = 0;
	int numSkipBkt = 0;
#endif

	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) / (1 - falsePositive)) / log(avgWidth) * subs;
	//	maxSkipPredicate/=2;
	cout << "k3_local= " << maxSkipPredicate << "\n";

	int skipIndex = 0;

	for (skipIndex = 0; skipIndex < atts; skipIndex++)
	{
		if (numSkipPredicate + attsCounts[skipIndex].count < maxSkipPredicate)
		{
			numSkipPredicate = numSkipPredicate + attsCounts[skipIndex].count;
			skipped[attsCounts[skipIndex].att] = true;
#ifdef DEBUG
			numSkipAttr++;
			// cout << "Skip Attribute " << attsCounts[skipIndex].att<<"\n"; // could output in finding order.
#endif // DEBUG
		}
		else
		{
			break;
		}
	}

	// <low0/high1, AttributeId, bucketId, sizeOfBucket>
	auto cmp = [&](const auto &a, const auto &b)
	{
		return get<3>(a) > get<3>(b); // aͰС, ����false, �Ͱ�a��Ϊ��, ��ʵ��С����
	};
	priority_queue<tuple<int, int, int, int>, vector<tuple<int, int, int, int>>, decltype(cmp)> minHeap(cmp);
	while (skipIndex < atts)
	{
		minHeap.push(
			make_tuple(0, attsCounts[skipIndex].att, buks - 1, data[0][attsCounts[skipIndex].att][buks - 1].size()));
		minHeap.push(make_tuple(1, attsCounts[skipIndex].att, 0, data[1][attsCounts[skipIndex].att][0].size()));
		skipIndex++;
	}
	while (true)
	{
		auto item = minHeap.top();
		minHeap.pop();
		if (numSkipPredicate + get<3>(item) / 2 > maxSkipPredicate)
		{
			break;
		}
		numSkipPredicate = numSkipPredicate + get<3>(item) / 2;
#ifdef DEBUG
		numSkipBkt++;
#endif
		if (get<0>(item) == 0)
		{											   // low
			get<2>(item) -= 1;						   // ���˵����Ͱ
			endBucket[0][get<1>(item)] = get<2>(item); // ���˵����Ͱ
			if (get<2>(item) >= 1)
			{															   // ����ʣ���Ͱ���Ա�����, Ϊ0ʱֹͣ, ��Ϊ0��Ͱ��LVE�϶����ǱȽ�Ͱ�������Ǳ��Ͱ
				get<3>(item) = data[0][get<1>(item)][get<2>(item)].size(); // ����Ͱ��С
				minHeap.push(item);
			}
		}
		else
		{											   // high
			get<2>(item) += 1;						   // ���˵����Ͱ
			endBucket[1][get<1>(item)] = get<2>(item); // ���˵����Ͱ
			if (get<2>(item) < buks - 1)
			{ // ����ʣ���Ͱ���Ա�����
				get<3>(item) = data[1][get<1>(item)][get<2>(item)].size();
				minHeap.push(item);
			}
		}
	}
#ifdef DEBUG
	cout << "In theory, rightMatchNum= " << pow(width, avgSubSize) * subs << ", wrongMatchNum= "
		 << pow(width, avgSubSize) * subs / (1 - falsePositive) * falsePositive << ", falsePositiveRate_local= "
		 << falsePositive
		 << ", falsePositiveRate_global= " << falsePositiveRate_global << ".\n";
	cout << "avgSubSize= " << avgSubSize << ", "
		 << "avgWidth= " << avgWidth << ", numPredicate= " << numPredicate
		 << ", maxSkipPredicate= " << maxSkipPredicate << ", numSkipPredicate= " << numSkipPredicate << ".\n";
	cout << "Total skipped attribute: " << numSkipAttr << " among " << atts << " attributes.\n";
	cout << "Total skipped bucket: " << numSkipAttr << "*2*" << buks << " + " << numSkipBkt << " = "
		 << numSkipAttr * 2 * buks + numSkipBkt << " among " << atts * 2 * buks << " buckets.\n";
	cout << "Skip attribute:";
	_for(i, 0, atts) if (skipped[i])
			cout
		<< " " << i;
	cout << "\n";
#endif
}

void AdaRein::approx_match_sss(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	bitset<subs> bits;
	vector<bool> attExist(atts, false);
	for (auto &&iPair : pub.pairs)
	{
		int att = iPair.att;
		attExist[att] = true;
		if (skipped[att])
			continue;
		int value = iPair.value, buck = value / buckStep;

		for (int k = 0; k < data[0][att][buck].size(); k++)
			if (data[0][att][buck][k].val > value)
				bits[data[0][att][buck][k].subID] = true;
		for (int j = buck + 1; j <= endBucket[0][att]; j++) // ��HEMϵ�е���Ʋ�ͬ, ����ȡ������
			for (int k = 0; k < data[0][att][j].size(); k++)
				bits[data[0][att][j][k].subID] = true;

		for (int k = 0; k < data[1][att][buck].size(); k++)
			if (data[1][att][buck][k].val < value)
				bits[data[1][att][buck][k].subID] = true;
		for (int j = buck - 1; j >= endBucket[1][att]; j--)
			for (int k = 0; k < data[1][att][j].size(); k++)
				bits[data[1][att][j][k].subID] = true;
	}

	// �����滻Ϊ1��λ����
	for (int i = 0; i < atts; i++)
		if (!attExist[i] && !skipped[i])
			for (int j = buks - 1; j >= endBucket[1][i]; j--)
				for (int k = 0; k < data[1][i][j].size(); k++)
					bits[data[1][i][j][k].subID] = true;

	matchSubs = subs - bits.count();
}

void AdaRein::static_succession_selection_backward(double falsePositive, const vector<IntervalSub> &subList)
{
	for (int i = 0; i < atts; i++)
	{
		attsCounts[i].att = i;
		attsCounts[i].count = 0;
	}

	int numPredicate = 0, numSkipPredicate = 0; // ν������, �ѹ��˵�ν������
	double avgSubSize = 0, avgWidth = 0;		// ƽ��ÿ�������ж��ٸ�ν��, ν�ʵ�ƽ������
	for (auto &&iSub : subList)
	{
		// numPredicate += iSub.constraints.size(); // ����
		for (auto &&iCnt : iSub.constraints)
		{
			//++attsCounts[iCnt.att].count; // ����
			avgWidth += iCnt.highValue - iCnt.lowValue;
		}
	}
	// ��Ҫ�Ȳ��붩�Ĳ�����ô��
	_for(i, 0, atts)
	{
		_for(j, 0, buks) attsCounts[i].count += data[0][i][j].size();
		numPredicate += attsCounts[i].count;
	}
	avgSubSize = (double)numPredicate / subList.size();
	avgWidth /= (double)numPredicate * valDom;

	sort(attsCounts.begin(), attsCounts.end());

	// minPredicate �����Դ���
	// double minK = log(pow(avgWidth / valDom, avgSubSize) + falsePositive) / log(avgWidth / valDom);
	// inline auto valid = [&](double) {return (double)(numPredicate - numSkipPredicate) > minK * subs; };

	double falsePositiveRate_global = pow(avgWidth, avgSubSize) * subs / (1 - falsePositive) * falsePositive / subs;

	maxSkipPredicate = numPredicate - (avgSubSize - log(1 - falsePositive) / log(avgWidth)) * subs; // k2

#ifdef DEBUG
	cout << "k2= " << maxSkipPredicate << "\n";
	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) + falsePositiveRate_global) / log(avgWidth) *
						   subs; // ������Թ��˵�ν����, currentSum�����ֵ
	cout << "k3_global= " << maxSkipPredicate << "\n";
	int numSkipAttr = 0;
	int numSkipBkt = 0;
#endif

	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) / (1 - falsePositive)) / log(avgWidth) * subs;
	//	maxSkipPredicate *= 6;
	cout << "k3_local= " << maxSkipPredicate << "\n";

	int skipIndex = 0;

	for (skipIndex = 0; skipIndex < atts; skipIndex++)
	{
		if (numSkipPredicate + attsCounts[skipIndex].count < maxSkipPredicate)
		{
			numSkipPredicate = numSkipPredicate + attsCounts[skipIndex].count;
			skipped[attsCounts[skipIndex].att] = true;
#ifdef DEBUG
			numSkipAttr++;
			// cout << "Skip Attribute " << attsCounts[skipIndex].att<<"\n"; // could output in finding order.
#endif // DEBUG
		}
		else
		{
			break;
		}
	}

	// <low0/high1, AttributeId, bucketId, sizeOfBucket>
	auto cmp = [&](const auto &a, const auto &b)
	{
		return get<3>(a) > get<3>(b); // aͰС, ����false, �Ͱ�a��Ϊ��, ��ʵ��С����
	};
	priority_queue<tuple<int, int, int, int>, vector<tuple<int, int, int, int>>, decltype(cmp)> minHeap(cmp);
	while (skipIndex < atts)
	{
		minHeap.push(
			make_tuple(0, attsCounts[skipIndex].att, 0, data[0][attsCounts[skipIndex].att][0].size()));
		minHeap.push(
			make_tuple(1, attsCounts[skipIndex].att, buks - 1, data[1][attsCounts[skipIndex].att][buks - 1].size()));
		skipIndex++;
	}

	while (true)
	{
		auto item = minHeap.top();
		minHeap.pop();
		if (numSkipPredicate + get<3>(item) / 2 > maxSkipPredicate)
		{
			break;
		}
		numSkipPredicate = numSkipPredicate + get<3>(item) / 2;
#ifdef DEBUG
		numSkipBkt++;
#endif
		if (get<0>(item) == 0)
		{												 // low
			get<2>(item) += 1;							 // ���˵����Ͱ
			beginBucket[0][get<1>(item)] = get<2>(item); // ���˵����Ͱ
			if (get<2>(item) < buks - 1)
			{ // ����ʣ���Ͱ���Ա�����
				get<3>(item) = data[0][get<1>(item)][get<2>(item)].size();
				minHeap.push(item);
			}
		}
		else
		{												 // high
			get<2>(item) -= 1;							 // ���˵����Ͱ
			beginBucket[1][get<1>(item)] = get<2>(item); // ���˵����Ͱ
			if (get<2>(item) > 0)
			{															   // ����ʣ���Ͱ���Ա�����, Ϊ0ʱֹͣ, ��Ϊ0��Ͱ��LVE�϶����ǱȽ�Ͱ�������Ǳ��Ͱ
				get<3>(item) = data[1][get<1>(item)][get<2>(item)].size(); // ����Ͱ��С
				minHeap.push(item);
			}
		}
	}
#ifdef DEBUG
	cout << "In theory, rightMatchNum= " << pow(width, avgSubSize) * subs << ", wrongMatchNum= "
		 << pow(width, avgSubSize) * subs / (1 - falsePositive) * falsePositive << ", falsePositiveRate_local= "
		 << falsePositive
		 << ", falsePositiveRate_global= " << falsePositiveRate_global << ".\n";
	cout << "avgSubSize= " << avgSubSize << ", "
		 << "avgWidth= " << avgWidth << ", numPredicate= " << numPredicate
		 << ", maxSkipPredicate= " << maxSkipPredicate << ", numSkipPredicate= " << numSkipPredicate << ".\n";
	cout << "Total skipped attribute: " << numSkipAttr << " among " << atts << " attributes.\n";
	cout << "Total skipped bucket: " << numSkipAttr << "*2*" << buks << " + " << numSkipBkt << " = "
		 << numSkipAttr * 2 * buks + numSkipBkt << " among " << atts * 2 * buks << " buckets.\n";
	cout << "Skip attribute:";
	_for(i, 0, atts) if (skipped[i])
			cout
		<< " " << i;
	cout << "\n";
#endif
}

void AdaRein::approx_match_sss_b(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	bitset<subs> bits;
	vector<bool> attExist(atts, false);
	for (auto &&iPair : pub.pairs)
	{
		int att = iPair.att;
		attExist[att] = true;
		if (skipped[att])
			continue;
		int value = iPair.value, buck = value / buckStep;

		for (int k = 0; k < data[0][att][buck].size(); k++)
			if (data[0][att][buck][k].val > value)
				bits[data[0][att][buck][k].subID] = true;
		for (int j = max(buck + 1, beginBucket[0][att]); j < buks; j++) // ��HEMϵ�е���Ʋ�ͬ, ����ȡ������
			for (int k = 0; k < data[0][att][j].size(); k++)
				bits[data[0][att][j][k].subID] = true;

		for (int k = 0; k < data[1][att][buck].size(); k++)
			if (data[1][att][buck][k].val < value)
				bits[data[1][att][buck][k].subID] = true;
		for (int j = min(buck - 1, beginBucket[1][att]); j >= 0; j--)
			for (int k = 0; k < data[1][att][j].size(); k++)
				bits[data[1][att][j][k].subID] = true;
	}

	// �����滻Ϊ1��λ����
	for (int i = 0; i < atts; i++)
		if (!attExist[i] && !skipped[i])
			for (int j = beginBucket[1][i]; j >= 0; j--)
				for (auto &&k : data[1][i][j])
					bits[k.subID] = true;

	matchSubs = subs - bits.count();
}

void AdaRein::static_succession_selection_crossed(double falsePositive, const vector<IntervalSub> &subList)
{
	for (int i = 0; i < atts; i++)
	{
		attsCounts[i].att = i;
		attsCounts[i].count = 0;
	}

	int numPredicate = 0, numSkipPredicate = 0; // ν������, �ѹ��˵�ν������
	double avgSubSize = 0, avgWidth = 0;		// ƽ��ÿ�������ж��ٸ�ν��, ν�ʵ�ƽ������
	for (auto &&iSub : subList)
	{
		// numPredicate += iSub.constraints.size(); // ����
		for (auto &&iCnt : iSub.constraints)
		{
			//++attsCounts[iCnt.att].count; // ����
			avgWidth += iCnt.highValue - iCnt.lowValue;
		}
	}
	// ��Ҫ�Ȳ��붩�Ĳ�����ô��
	_for(i, 0, atts)
	{
		_for(j, 0, buks) attsCounts[i].count += data[0][i][j].size();
		numPredicate += attsCounts[i].count;
	}
	avgSubSize = (double)numPredicate / subList.size();
	avgWidth /= (double)numPredicate * valDom;

	sort(attsCounts.begin(), attsCounts.end());

	// minPredicate �����Դ���
	// double minK = log(pow(avgWidth / valDom, avgSubSize) + falsePositive) / log(avgWidth / valDom);
	// inline auto valid = [&](double) {return (double)(numPredicate - numSkipPredicate) > minK * subs; };

	double falsePositiveRate_global = pow(avgWidth, avgSubSize) * subs / (1 - falsePositive) * falsePositive / subs;

	maxSkipPredicate = numPredicate - (avgSubSize - log(1 - falsePositive) / log(avgWidth)) * subs; // k2

#ifdef DEBUG
	cout << "k2= " << maxSkipPredicate << "\n";
	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) + falsePositiveRate_global) / log(avgWidth) *
						   subs; // ������Թ��˵�ν����, currentSum�����ֵ
	cout << "k3_global= " << maxSkipPredicate << "\n";
	int numSkipAttr = 0;
	int numSkipBkt = 0;
#endif

	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) / (1 - falsePositive)) / log(avgWidth) * subs;
	//	maxSkipPredicate *= 6;
	cout << "k3_local= " << maxSkipPredicate << "\n";

	int skipIndex = 0;
	for (skipIndex = 0; skipIndex < atts; skipIndex++)
	{
		if (numSkipPredicate + attsCounts[skipIndex].count < maxSkipPredicate)
		{
			numSkipPredicate = numSkipPredicate + attsCounts[skipIndex].count;
			skipped[attsCounts[skipIndex].att] = true;
#ifdef DEBUG
			numSkipAttr++;
			// cout << "Skip Attribute " << attsCounts[skipIndex].att<<"\n"; // could output in finding order.
#endif // DEBUG
		}
		else
		{
			break;
		}
	}

	// ���˿�Ͱ
	for (int i = 0; i < atts; i++)
	{
		while (beginBucket[0][i] < endBucket[0][i] && data[0][i][beginBucket[0][i]].size() == 0)
			beginBucket[0][i]++;
		while (beginBucket[0][i] < endBucket[0][i] && data[0][i][beginBucket[0][i]].size() == 0)
			endBucket[0][i]--;
		while (beginBucket[1][i] > endBucket[1][i] && data[1][i][beginBucket[1][i]].size() == 0)
			beginBucket[1][i]--;
		while (beginBucket[1][i] > endBucket[1][i] && data[1][i][endBucket[1][i]].size() == 0)
			endBucket[1][i]++;
#ifdef DEBUG
		numSkipBkt += beginBucket[0][i] + buks - 1 - endBucket[0][i] + endBucket[1][i] + buks - 1 - beginBucket[1][i];
#endif
	}

	// <low0/high1, AttributeId, bucketId, sizeOfBucket>
	auto cmp = [&](const auto &a, const auto &b)
	{
		return get<3>(a) > get<3>(b); // aͰС, ����false, �Ͱ�a��Ϊ��, ��ʵ��С����
	};
	priority_queue<tuple<int, int, int, int>, vector<tuple<int, int, int, int>>, decltype(cmp)> minHeap(cmp);
	vector<bool> end[2];		// low/high -> att -> 0: ����С�Ŷˣ�1�����˴�Ŷ�
	end[0].resize(atts, true);	// ��ֵ�˴Ӵ�Ŷ�(������)��ʼ����
	end[1].resize(atts, false); // ��ֵ�˴�С��Ͱ��(������)��ʼ����
	while (skipIndex < atts)
	{
		int &att = attsCounts[skipIndex].att;
		if (endBucket[0][att] > beginBucket[0][att])
			minHeap.push(make_tuple(0, att, endBucket[0][att],
									data[0][att][endBucket[0][att]].size()));
		if (endBucket[1][att] < beginBucket[0][att])
			minHeap.push(make_tuple(1, att, endBucket[1][att],
									data[1][att][endBucket[1][att]].size()));
		skipIndex++;
	}

	while (true)
	{
		auto item = minHeap.top();
		minHeap.pop();
		if (numSkipPredicate + get<3>(item) / 2 > maxSkipPredicate)
		{
			break;
		}
		numSkipPredicate = numSkipPredicate + get<3>(item) / 2;
#ifdef DEBUG
		numSkipBkt++;
#endif
		if (get<0>(item) == 0)
		{ // low
			if (end[0][get<1>(item)])
			{ // ���Ͱ
				get<2>(item) -= 1;
				endBucket[0][get<1>(item)] = get<2>(item); // ���˵����Ͱ
				if (get<2>(item) >= beginBucket[0][get<1>(item)])
				{
					get<2>(item) = beginBucket[0][get<1>(item)]; // ת��
					get<3>(item) = data[0][get<1>(item)][get<2>(item)].size();
					minHeap.push(item);
				}
			}
			else
			{												 // С��Ͱ
				get<2>(item) += 1;							 // ���˵����Ͱ
				beginBucket[0][get<1>(item)] = get<2>(item); // ���˵����Ͱ
				if (get<2>(item) <= endBucket[0][get<1>(item)])
				{											   // ����ʣ���Ͱ���Ա�����
					get<2>(item) = endBucket[0][get<1>(item)]; // ת��
					get<3>(item) = data[0][get<1>(item)][get<2>(item)].size();
					minHeap.push(item);
				}
			}
			end[0][get<1>(item)] = !end[0][get<1>(item)];
		}
		else
		{ // high
			if (end[1][get<1>(item)])
			{												 // ���Ͱ(beginBucket)
				get<2>(item) -= 1;							 // ���˵����Ͱ
				beginBucket[1][get<1>(item)] = get<2>(item); // ���˵�������Ͱ
				if (get<2>(item) >= endBucket[1][get<1>(item)])
				{															   // ����ʣ���Ͱ���Ա�����
					get<2>(item) = endBucket[1][get<1>(item)];				   // ת��
					get<3>(item) = data[1][get<1>(item)][get<2>(item)].size(); // ����Ͱ��С
					minHeap.push(item);
				}
			}
			else
			{											   // С��Ͱ(endBucket)
				get<2>(item) += 1;						   // ���˵����Ͱ
				endBucket[1][get<1>(item)] = get<2>(item); // ���˵����Ͱ
				if (get<2>(item) <= beginBucket[1][get<1>(item)])
				{												 // ����ʣ���Ͱ���Ա�����
					get<2>(item) = beginBucket[1][get<1>(item)]; // ת��
					get<3>(item) = data[1][get<1>(item)][get<2>(item)].size();
					minHeap.push(item);
				}
			}
			end[1][get<1>(item)] = !end[1][get<1>(item)];
		}
	}

	// �����ɢ�Ķ�Ͱ������������ȫ��������, �ͱ�ǵ�sekippedW��
	_for(i, 0, atts)
	{
		if (beginBucket[0][i] >= endBucket[0][i] && beginBucket[1][i] <= endBucket[1][i])
		{
			printf(
				"Error: if %d is a full-skipped attribute, it would already be skipped before discretization selection.",
				i);
			skipped[i] = true;
#ifdef DEBUG
			numSkipAttr++; // ������ϵĻ�, numSkipBkt�Ͳ�׼ȷ��
#endif					   // DEBUG
		}
	}

#ifdef DEBUG
	cout << "In theory, rightMatchNum= " << pow(width, avgSubSize) * subs << ", wrongMatchNum= "
		 << pow(width, avgSubSize) * subs / (1 - falsePositive) * falsePositive << ", falsePositiveRate_local= "
		 << falsePositive
		 << ", falsePositiveRate_global= " << falsePositiveRate_global << ".\n";
	cout << "avgSubSize= " << avgSubSize << ", "
		 << "avgWidth= " << avgWidth << ", numPredicate= " << numPredicate
		 << ", maxSkipPredicate= " << maxSkipPredicate << ", numSkipPredicate= " << numSkipPredicate << ".\n";
	cout << "Total skipped attribute: " << numSkipAttr << " among " << atts << " attributes.\n";
	cout << "Total skipped bucket: " << numSkipAttr << "*2*" << buks << " + " << numSkipBkt << " = "
		 << numSkipAttr * 2 * buks + numSkipBkt << " among " << atts * 2 * buks << " buckets.\n";
	cout << "Skip attribute:";
	_for(i, 0, atts) if (skipped[i])
			cout
		<< " " << i;
	cout << "\n";
#endif
}

void AdaRein::approx_match_sss_c(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	bitset<subs> bits;
	vector<bool> attExist(atts, false);
	for (auto &&iPair : pub.pairs)
	{
		int att = iPair.att;
		attExist[att] = true;
		if (skipped[att])
			continue;
		int value = iPair.value, buck = value / buckStep;

		for (int k = 0; k < data[0][att][buck].size(); k++)
			if (data[0][att][buck][k].val > value)
				bits[data[0][att][buck][k].subID] = true;
		for (int j = max(buck + 1, beginBucket[0][att]); j <= endBucket[0][att]; j++)
			for (auto &&k : data[0][att][j])
				bits[k.subID] = true;

		for (int k = 0; k < data[1][att][buck].size(); k++)
			if (data[1][att][buck][k].val < value)
				bits[data[1][att][buck][k].subID] = true;
		for (int j = min(buck - 1, beginBucket[1][att]); j >= endBucket[1][att]; j--)
			for (auto &&k : data[1][att][j])
				bits[k.subID] = true;
	}

	// �����滻Ϊ1��λ����
	for (int i = 0; i < atts; i++)
		if (!attExist[i] && !skipped[i])
			for (int j = beginBucket[1][i]; j >= max(0, endBucket[1][i]); j--)
				for (auto &&k : data[1][i][j])
					bits[k.subID] = true;

	matchSubs = subs - bits.count();
}

// void AdaRein::insert_sss_c_pph(IntervalSub sub) {
//	Combo c;
//	int levelId = -1, divider = 0.2;
//	for (const auto& cnt:sub.constraints) {
//		c.subID = sub.id;
//		c.val = cnt.lowValue;
//		if (cnt.highValue - cnt.lowValue < dividerValue) {
//			dataL[0][0][cnt.att][c.val / buckStep].push_back(c);
//			c.val = cnt.highValue;
//			dataL[0][1][cnt.att][c.val / buckStep].push_back(c);
//		}
//		else {
//			dataL[1][0][cnt.att][c.val / buckStep].push_back(c);
//			c.val = cnt.highValue;
//			dataL[1][1][cnt.att][c.val / buckStep].push_back(c);
//		}
//	}
//	numSub++;
// }

void AdaRein::insert_sss_c_w(IntervalSub sub)
{
	Combo c;
	c.subID = sub.id;
	int levelId = -1;
	for (const auto &cnt : sub.constraints)
	{
		c.val = cnt.lowValue;
		levelId = (cnt.highValue - cnt.lowValue) / widthStep;			// widthStep:���ڲ�֮��ν�������ȵĲ�
		dataW[cnt.att][levelId][0][c.val / levelBuckStep].push_back(c); // levelBuckStep:ÿһ���ÿ��Ͱ�������������
		c.val = cnt.highValue;
		dataW[cnt.att][levelId][1][c.val / levelBuckStep].push_back(c);
	}
	numSub++;
}

void AdaRein::static_succession_selection_crossed_width(double falsePositive, const vector<IntervalSub> &subList)
{

	int numPredicate = 0, numSkipPredicate = 0; // ν������, �ѹ��˵�ν������
	double avgSubSize = 0, avgWidth = 0;		// ƽ��ÿ�������ж��ٸ�ν��, ν�ʵ�ƽ������
	for (auto &&iSub : subList)
	{
		numPredicate += iSub.constraints.size();
		for (auto &&iCnt : iSub.constraints)
		{
			//++attsCounts[iCnt.att].count;
			avgWidth += iCnt.highValue - iCnt.lowValue;
		}
	}
	avgSubSize = (double)numPredicate / subList.size();
	avgWidth /= (double)numPredicate * valDom; // ��һ��

	// minPredicate �����Դ���
	// double minK = log(pow(avgWidth, avgSubSize) + falsePositive) / log(avgWidth);
	// inline auto valid = [&](double) {return (double)(numPredicate - numSkipPredicate) > minK * subs; };

	double falsePositiveRate_global = pow(avgWidth, avgSubSize) * subs / (1 - falsePositive) * falsePositive / subs;
	// falsePositiveRate_global=pow(avgWidth, avgSubSize)  / (1 - falsePositive) * falsePositive
	maxSkipPredicate = numPredicate - (avgSubSize - log(1 - falsePositive) / log(avgWidth)) * subs; // k2

#ifdef DEBUG
	cout << "k2= " << maxSkipPredicate << "\n";
	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) + falsePositiveRate_global) / log(avgWidth) *
						   subs; // ������Թ��˵�ν����, numSkipPredicate�����ֵ
	cout << "k3_global= " << maxSkipPredicate << "\n";
	int numSkipAttr = 0;
	int numSkipBkt = 0;
#endif

	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) / (1 - falsePositive)) / log(avgWidth) * subs;
	//	maxSkipPredicate *= 6;
	cout << "k3_local= " << maxSkipPredicate << "\n";

	int skipWidthIndex = adarein_level - 1; // �������ʲ㿪ʼ����
	while (skipWidthIndex >= 0)
	{
		//		cout << skipWidthIndex << "\n";
		// ͳ������ϵĸ������ϵ�ν������
		for (int i = 0; i < atts; i++)
		{
			attsCounts[i].att = i;
			attsCounts[i].count = 0;
		}
		for (int i = 0; i < atts; i++)
		{
			for (int j = 0; j < levelBuks; j++)								  // ÿһ���Ͱ��
				attsCounts[i].count += dataW[i][skipWidthIndex][0][j].size(); // attr->level->low/high->bucketId->offset
		}
		sort(attsCounts.begin(), attsCounts.end());

		// ��̬�������Ҹò��ϵ�ȫ��������
		int skipIndex = 0;
		for (skipIndex = 0; skipIndex < atts; skipIndex++)
		{
			//			cout << "s= " << skipIndex << "\n";
			if (numSkipPredicate + attsCounts[skipIndex].count < maxSkipPredicate)
			{
				//				cout << "Skip Attribute " << attsCounts[skipIndex].att<<" on widthIndex "<<skipWidthIndex<<"\n"; // could output in finding order.
				numSkipPredicate = numSkipPredicate + attsCounts[skipIndex].count;
				skippedW[attsCounts[skipIndex].att][skipWidthIndex] = true;
#ifdef DEBUG
				numSkipAttr++; // ���˲�ͬ�����ϵ�ͬһ���Ի�ƶ��
#endif						   // DEBUG
			}
			else
			{
				break;
			}
		}

		// ����ÿһ���ϵĿ�Ͱ   ��һ���ǳ�ʼ������ռƥ��ʱ�䡣
		for (int i = skipIndex; i < atts; i++)
		{
			auto &beBW = beBucketW[i][skipWidthIndex];	  // attr->level-><low,high>-><begin,end>
			const auto &dataw = dataW[i][skipWidthIndex]; // SSS-C-PPH SSS-C-W: attr->level->low/high->bucketId->offset
			while (beBW.first.first < beBW.first.second && dataw[0][beBW.first.first].size() == 0)
				beBW.first.first++;
			while (beBW.first.first < beBW.first.second && dataw[0][beBW.first.second].size() == 0)
				beBW.first.second--;
			while (beBW.second.first > beBW.second.second && dataw[1][beBW.second.first].size() == 0)
				beBW.second.first--;
			while (beBW.second.first > beBW.second.second && dataw[1][beBW.second.second].size() == 0)
				beBW.second.second++;
#ifdef DEBUG
			numSkipBkt += beBW.first.first + levelBuks - 1 - beBW.first.second + beBW.second.second + levelBuks - 1 -
						  beBW.second.first;
#endif
		}

		//		printf("skipWidthIndex=%d, skipIndex=%d\n", skipWidthIndex, skipIndex);

		// ��������Ͱ����
		// <low0/high1, AttributeId, bucketId, sizeOfBucket>
		auto cmp = [&](const auto &a,
					   const auto &b)
		{
			return get<3>(a) > get<3>(b); // aͰС, ����false, �Ͱ�a��Ϊ��, ��ʵ��С����
		};
		priority_queue<tuple<int, int, int, int>, vector<tuple<int, int, int, int>>, decltype(cmp)> minHeap(cmp);
		vector<bool> end[2];		// ��¼�������С��Ͱ�˻��Ǵ��Ͱ��: low/high -> att -> 0: ����С��Ͱ�ˣ�1�����˴��Ͱ��
		end[0].resize(atts, true);	// ��ֵ�˴Ӵ��Ͱ��(������)��ʼ����
		end[1].resize(atts, false); // ��ֵ�˴�С��Ͱ��(������)��ʼ����
		while (skipIndex < atts)
		{
			const int &att = attsCounts[skipIndex].att;
			auto &beBW = beBucketW[att][skipWidthIndex];	// attr->level-><low,high>-><begin,end>
			const auto &dataw = dataW[att][skipWidthIndex]; // SSS-C-PPH SSS-C-W: attr->level->low/high->bucketId->offset
			if (beBW.first.first < beBW.first.second)		// ��ֵ�˻��й��˿ռ�
				minHeap.push(
					make_tuple(0, att, beBW.first.second,
							   dataw[0][beBW.first.second].size()));
			if (beBW.second.first > beBW.second.second) // ��ֵ�˻��й��˿ռ�
				minHeap.push(
					make_tuple(1, att, beBW.second.second,
							   dataw[1][beBW.second.second].size()));
			skipIndex++;
		}

		// �����ÿ��Ȳ��ϵĲ��ֹ�������, ���˶���Ķ�Ͱ
		while (!minHeap.empty())
		{
			auto item = minHeap.top();
			minHeap.pop();
			if (numSkipPredicate + get<3>(item) / 2 > maxSkipPredicate)
			{
				break;
			}
			numSkipPredicate = numSkipPredicate + get<3>(item) / 2;
#ifdef DEBUG
			numSkipBkt++;
#endif
			const int &att = get<1>(item);
			int &bktId = get<2>(item);
			auto &beBW = beBucketW[att][skipWidthIndex];
			const auto &dataw = dataW[att][skipWidthIndex];
			if (get<0>(item) == 0)
			{ // low
				if (end[0][att])
				{ // ���Ͱ�� endBucket
					bktId -= 1;
					beBW.first.second = bktId; // ���˵����Ͱ
					if (bktId > beBW.first.first)
					{
						bktId = beBW.first.first; // ת��
						get<3>(item) = dataw[0][bktId].size();
						minHeap.push(item);
					}
				}
				else
				{ // С��Ͱ�� beginBucket
					bktId += 1;
					beBW.first.first = bktId; // ���˵����Ͱ
					if (bktId < beBW.first.second)
					{							   // ����ʣ���Ͱ���Ա�����
						bktId = beBW.first.second; // ת��
						get<3>(item) = dataw[0][bktId].size();
						minHeap.push(item);
					}
				}
				end[0][get<1>(item)] = !end[0][get<1>(item)];
			}
			else
			{ // high
				if (end[1][att])
				{ // ���Ͱ beginBucket
					bktId -= 1;
					beBW.second.first = bktId; // ���˵�������Ͱ
					if (bktId > beBW.second.second)
					{										   // ����ʣ���Ͱ���Ա�����
						bktId = beBW.second.second;			   // ת��
						get<3>(item) = dataw[1][bktId].size(); // ����Ͱ��С
						minHeap.push(item);
					}
				}
				else
				{								// С��Ͱ endBucket
					bktId += 1;					// ���˵����Ͱ
					beBW.second.second = bktId; // ���˵����С��Ͱ
					if (bktId < beBW.second.first)
					{							   // ����ʣ���Ͱ���Ա�����
						bktId = beBW.second.first; // ת��
						get<3>(item) = dataw[1][bktId].size();
						minHeap.push(item);
					}
				}
				end[1][get<1>(item)] = !end[1][get<1>(item)];
			} // high
		}	  // ��
		skipWidthIndex--;
	} // ���Ȳ�

	// �����ɢ�Ķ�Ͱ������������ȫ��������, �ͱ�ǵ�sekippedW��
	_for(i, 0, atts)
	{
		_for(j, 0, adarein_level)
		{
			if (beBucketW[i][j].first.first >= beBucketW[i][j].first.second &&
				beBucketW[i][j].second.first <= beBucketW[i][j].second.second)
			{
				printf(
					"Error: if %d is a full-skipped attribute on width level %d, it would already be skipped before discretization selection.\n",
					i, j);
				skippedW[i][j] = true;
#ifdef DEBUG
				numSkipAttr++; // ������ϵĻ�, numSkipBkt�Ͳ�׼ȷ��
#endif						   // DEBUG
			}
		}
	}

#ifdef DEBUG
	cout << "In theory, rightMatchNum= " << pow(width, avgSubSize) * (double)subs << ", wrongMatchNum= "
		 << pow(width, avgSubSize) * (double)subs / (1 - falsePositive) * falsePositive
		 << ", falsePositiveRate_local= "
		 << falsePositive
		 << ", falsePositiveRate_global= " << falsePositiveRate_global << ".\n";
	cout << "avgSubSize= " << avgSubSize << ", "
		 << "avgWidth= " << avgWidth << ", numPredicate= " << numPredicate
		 << ", maxSkipPredicate= " << maxSkipPredicate << ", numSkipPredicate= " << numSkipPredicate << ".\n";
	cout << "Total skipped attribute on all widths: " << numSkipAttr << " among " << adarein_level << " widths of "
		 << atts << " attributes.\n";
	cout << "Total skipped bucket: " << numSkipAttr << "*2*" << levelBuks << " + " << numSkipBkt << " = "
		 << numSkipAttr * 2 * levelBuks + numSkipBkt << " among " << atts * adarein_level * 2 * levelBuks
		 << " buckets.\n";
	cout << "Skip attribute:";
	_for(i, 0, atts)
	{
		_for(j, 0, adarein_level)
		{
			if (skippedW[i][j])
			{
				cout << " a" << i << "w" << j;
			}
		}
		cout << ";  ";
	}
	cout << "\n";
#endif
}

void AdaRein::approx_match_sss_c_w(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	bitset<subs> bits;
	vector<bool> attExist(atts, false);
	for (auto &&iPair : pub.pairs)
	{
		int att = iPair.att;
		int value = iPair.value, buck = value / levelBuckStep;
		attExist[att] = true;

		_for(wi, 0, adarein_level)
		{
			if (skippedW[att][wi])
				continue;

			for (auto cmb : dataW[att][wi][0][buck])
				if (cmb.val > value)
					bits[cmb.subID] = true;
			for (int j = max(buck + 1, beBucketW[att][wi].first.first);
				 j <= beBucketW[att][wi].first.second; j++) // ��HEMϵ�е���Ʋ�ͬ, ����ȡ������
				for (auto &&k : dataW[att][wi][0][j])
					bits[k.subID] = true;

			for (auto cmb : dataW[att][wi][1][buck])
				if (cmb.val < value)
					bits[cmb.subID] = true;
			for (int j = min(buck - 1, beBucketW[att][wi].second.first);
				 j >= beBucketW[att][wi].second.second; j--)
				for (auto &&k : dataW[att][wi][1][j])
					bits[k.subID] = true;
		}
	}

	// �����滻Ϊ1��λ����
	for (int ai = 0; ai < atts; ai++)
		if (!attExist[ai])
		{
			for (int wi = 0; wi < adarein_level; wi++)
			{
				if (skippedW[ai][wi])
					continue;
				for (int j = beBucketW[ai][wi].second.first; j >= beBucketW[ai][wi].second.second; j--)
					for (auto &&k : dataW[ai][wi][1][j])
						bits[k.subID] = true;
			}
		}

	matchSubs = subs - bits.count();
}

void AdaRein::parallel_approx_match_sss_c_w(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{

	vector<future<bitset<subs>>> threadResult;
	int seg = pub.size / parallelDegree;
	int remainder = pub.size % parallelDegree;
	int tId = 0, end;
	for (int begin = 0; begin < pub.size; begin = end, tId++)
	{
		if (tId < remainder)
			end = begin + seg + 1;
		else
			end = begin + seg;
		threadResult.emplace_back(threadPool.enqueue([this, &pub, begin, end]
													 {
			//			printf("pub%d, begin=%d\n", pub.id, begin);
			bitset<subs> b; //=new bitset<subs>;
			for (int i = begin; i < end; i++) {
				int value = pub.pairs[i].value, att = pub.pairs[i].att, buck = value / levelBuckStep;
				_for(wi, 0, adarein_level) {
					if (skippedW[att][wi])
						continue;

					for (auto cmb : dataW[att][wi][0][buck])
						if (cmb.val > value)
							b[cmb.subID] = true;
					for (int j = max(buck + 1, beBucketW[att][wi].first.first);
						j <= beBucketW[att][wi].first.second; j++) // ��HEMϵ�е���Ʋ�ͬ, ����ȡ������
						for (auto&& k : dataW[att][wi][0][j])
							b[k.subID] = true;

					for (auto cmb : dataW[att][wi][1][buck])
						if (cmb.val < value)
							b[cmb.subID] = true;
					for (int j = min(buck - 1, beBucketW[att][wi].second.first);
						j >= beBucketW[att][wi].second.second; j--)
						for (auto&& k : dataW[att][wi][1][j])
							b[k.subID] = true;
				}
			}
			return b; }));
	}

	// �����滻Ϊ1��λ����
	bitset<subs> bits;
	if (pub.size < atts)
	{
		vector<bool> attExist(atts, false);
		for (const auto item : pub.pairs)
			attExist[item.att] = true;
		for (int ai = 0; ai < atts; ai++)
			if (!attExist[ai])
			{
				for (int wi = 0; wi < adarein_level; wi++)
				{
					if (skippedW[ai][wi])
						continue;
					for (int j = beBucketW[ai][wi].second.first; j >= beBucketW[ai][wi].second.second; j--)
						for (auto &&k : dataW[ai][wi][1][j])
							bits[k.subID] = true;
				}
			}
	}
	for (int i = 0; i < threadResult.size(); i++)
		bits |= threadResult[i].get();
	matchSubs = subs - bits.count();
}

void AdaRein::parallel2_static_succession_selection_crossed_width(double falsePositive, const vector<IntervalSub> &subList)
{
	int numPredicate = 0, numSkipPredicate = 0; // ν������, �ѹ��˵�ν������
	double avgSubSize = 0, avgWidth = 0;		// ƽ��ÿ�������ж��ٸ�ν��, ν�ʵ�ƽ������
	for (auto &&iSub : subList)
	{
		numPredicate += iSub.constraints.size();
		for (auto &&iCnt : iSub.constraints)
		{
			//++attsCounts[iCnt.att].count;
			avgWidth += iCnt.highValue - iCnt.lowValue;
		}
	}
	avgSubSize = (double)numPredicate / subList.size();
	avgWidth /= (double)numPredicate * valDom;

	// minPredicate �����Դ���
	// double minK = log(pow(avgWidth, avgSubSize) + falsePositive) / log(avgWidth);
	// inline auto valid = [&](double) {return (double)(numPredicate - numSkipPredicate) > minK * subs; };

	double falsePositiveRate_global = pow(avgWidth, avgSubSize) * subs / (1 - falsePositive) * falsePositive / subs;

	maxSkipPredicate = numPredicate - (avgSubSize - log(1 - falsePositive) / log(avgWidth)) * subs; // k2

#ifdef DEBUG
	cout << "k2= " << maxSkipPredicate << "\n";
	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) + falsePositiveRate_global) / log(avgWidth) *
						   subs; // ������Թ��˵�ν����, numSkipPredicate�����ֵ
	cout << "k3_global= " << maxSkipPredicate << "\n";
	int numSkipAttr = 0;
	int numSkipBkt = 0;
#endif

	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) / (1 - falsePositive)) / log(avgWidth) * subs;
	//	maxSkipPredicate *= 6;
	cout << "k3_local= " << maxSkipPredicate << "\n";

	int skipWidthIndex = adarein_level - 1; // �������ʲ㿪ʼ����
	while (skipWidthIndex >= 0)
	{
		// ͳ������ϵĸ������ϵ�ν������
		for (int i = 0; i < atts; i++)
		{
			attsCounts[i].att = i;
			attsCounts[i].count = 0;
		}
		for (int i = 0; i < atts; i++)
		{
			for (int j = 0; j < levelBuks; j++)
				attsCounts[i].count += dataW[i][skipWidthIndex][0][j].size();
		}
		sort(attsCounts.begin(), attsCounts.end());

		// ��̬�������Ҹò��ϵ�ȫ��������
		int skipIndex = 0;
		for (skipIndex = 0; skipIndex < atts; skipIndex++)
		{
			//			cout << "s= " << skipIndex << "\n";
			if (numSkipPredicate + attsCounts[skipIndex].count <= maxSkipPredicate)
			{
				//				cout << "Skip Attribute " << attsCounts[skipIndex].att<<" on widthIndex "<<skipWidthIndex<<"\n"; // could output in finding order.
				numSkipPredicate = numSkipPredicate + attsCounts[skipIndex].count;
				skippedW[attsCounts[skipIndex].att][skipWidthIndex] = true;
#ifdef DEBUG
				numSkipAttr++; // ���˲�ͬ�����ϵ�ͬһ���Ի�ƶ��
#endif						   // DEBUG
			}
			else
			{
				break;
			}
		}

		// ����ÿһ���ϵĿ�Ͱ
		for (int i = skipIndex; i < atts; i++)
		{
			auto &beBW = beBucketW[i][skipWidthIndex];
			const auto &dataw = dataW[i][skipWidthIndex];
			// ��Ϊ������Ե�����㲻����Ϊȫ���������ˣ��������ˣ�˵����ʣ���һ��Ͱ�����Ͱ�����ܱ����˵��ˣ����Բ���Ҫȡ�Ⱥ�
			while (beBW.first.first < beBW.first.second && dataw[0][beBW.first.first].size() == 0)
				beBW.first.first++;
			while (beBW.first.first < beBW.first.second && dataw[0][beBW.first.second].size() == 0)
				beBW.first.second--;
			while (beBW.second.first > beBW.second.second && dataw[1][beBW.second.first].size() == 0)
				beBW.second.first--;
			while (beBW.second.first > beBW.second.second && dataw[1][beBW.second.second].size() == 0)
				beBW.second.second++;
#ifdef DEBUG
			numSkipBkt += beBW.first.first + levelBuks - 1 - beBW.first.second + beBW.second.second + levelBuks - 1 -
						  beBW.second.first;
#endif
		}

		// ��������Ͱ����
		// <low0/high1, AttributeId, bucketId, sizeOfBucket>
		auto cmp = [&](const auto &a, const auto &b)
		{
			return get<3>(a) > get<3>(b); // aͰС, ����false, �Ͱ�a��Ϊ��, ��ʵ��С����
		};
		priority_queue<tuple<int, int, int, int>, vector<tuple<int, int, int, int>>, decltype(cmp)> minHeap(cmp);
		vector<bool> end[2];		// ��¼�������С��Ͱ�˻��Ǵ��Ͱ��: low/high -> att -> 0: ����С��Ͱ�ˣ�1�����˴��Ͱ��
		end[0].resize(atts, true);	// ��ֵ�˴Ӵ��Ͱ��(������)��ʼ����
		end[1].resize(atts, false); // ��ֵ�˴�С��Ͱ��(������)��ʼ����
		while (skipIndex < atts)
		{
			const int &att = attsCounts[skipIndex].att;
			auto &beBW = beBucketW[att][skipWidthIndex];
			const auto &dataw = dataW[att][skipWidthIndex];
			if (beBW.first.first <= beBW.first.second) // ��ֵ�˻��й��˿ռ�(must have)
				minHeap.push(
					make_tuple(0, att, beBW.first.second,
							   dataw[0][beBW.first.second].size()));
			if (beBW.second.first >= beBW.second.second) // ��ֵ�˻��й��˿ռ�(must have)
				minHeap.push(
					make_tuple(1, att, beBW.second.second,
							   dataw[1][beBW.second.second].size()));
			skipIndex++;
		}

		// �����ÿ��Ȳ��ϵĲ��ֹ�������, ���˶���Ķ�Ͱ
		while (!minHeap.empty())
		{
			auto item = minHeap.top();
			minHeap.pop();
			if (numSkipPredicate + get<3>(item) / 2 > maxSkipPredicate)
			{
				break;
			}
			numSkipPredicate = numSkipPredicate + get<3>(item) / 2;
#ifdef DEBUG
			numSkipBkt++;
#endif
			const int &att = get<1>(item);
			int &bktId = get<2>(item);
			auto &beBW = beBucketW[att][skipWidthIndex];
			const auto &dataw = dataW[att][skipWidthIndex];
			if (get<0>(item) == 0)
			{ // low
				if (end[0][att])
				{ // ���Ͱ�� endBucket
					bktId -= 1;
					beBW.first.second = bktId; // ���˵����Ͱ
					if (bktId > beBW.first.first)
					{
						bktId = beBW.first.first; // ת��
						get<3>(item) = dataw[0][bktId].size();
						minHeap.push(item);
					}
				}
				else
				{ // С��Ͱ�� beginBucket
					bktId += 1;
					beBW.first.first = bktId; // ���˵����Ͱ
					if (bktId < beBW.first.second)
					{							   // ����ʣ���Ͱ���Ա�����
						bktId = beBW.first.second; // ת��
						get<3>(item) = dataw[0][bktId].size();
						minHeap.push(item);
					}
				}
				end[0][att] = !end[0][att];
			}
			else
			{ // high
				if (end[1][att])
				{ // ���Ͱ beginBucket
					bktId -= 1;
					beBW.second.first = bktId; // ���˵�������Ͱ
					if (bktId > beBW.second.second)
					{										   // ����ʣ���Ͱ���Ա�����
						bktId = beBW.second.second;			   // ת��
						get<3>(item) = dataw[1][bktId].size(); // ����Ͱ��С
						minHeap.push(item);
					}
				}
				else
				{								// С��Ͱ endBucket
					bktId += 1;					// ���˵����Ͱ
					beBW.second.second = bktId; // ���˵����С��Ͱ
					if (bktId < beBW.second.first)
					{							   // ����ʣ���Ͱ���Ա�����
						bktId = beBW.second.first; // ת��
						get<3>(item) = dataw[1][bktId].size();
						minHeap.push(item);
					}
				}
				end[1][att] = !end[1][att];
			} // high
		}	  // ��
		skipWidthIndex--;
	} // ���Ȳ�

	//	// �����ɢ�Ķ�Ͱ������������ȫ��������, �ͱ�ǵ�sekippedW��
	//	_for(i, 0, atts) {
	//		_for(j, 0, adarein_level) {
	//			if (beBucketW[i][j].first.first > beBucketW[i][j].first.second &&
	//				beBucketW[i][j].second.first < beBucketW[i][j].second.second) {
	//				printf(
	//					"Error: if %d is a full-skipped attribute on width level %d, it would already be skipped before discretization selection.\n",
	//					i, j);
	//				skippedW[i][j] = true;
	// #ifdef DEBUG
	//				numSkipAttr++; // ������ϵĻ�, numSkipBkt�Ͳ�׼ȷ��
	// #endif // DEBUG
	//			}
	//		}
	//	}

#ifdef DEBUG
	cout << "In theory, rightMatchNum= " << pow(width, avgSubSize) * (double)subs << ", wrongMatchNum= "
		 << pow(width, avgSubSize) * (double)subs / (1 - falsePositive) * falsePositive
		 << ", falsePositiveRate_local= "
		 << falsePositive
		 << ", falsePositiveRate_global= " << falsePositiveRate_global << ".\n";
	cout << "avgSubSize= " << avgSubSize << ", "
		 << "avgWidth= " << avgWidth << ", numPredicate= " << numPredicate
		 << ", maxSkipPredicate= " << maxSkipPredicate << ", numSkipPredicate= " << numSkipPredicate << ".\n";
	cout << "Total skipped attribute on all widths: " << numSkipAttr << " among " << adarein_level << " widths of "
		 << atts << " attributes.\n";
	cout << "Total skipped bucket: " << numSkipAttr << "*2*" << levelBuks << " + " << numSkipBkt << " = "
		 << numSkipAttr * 2 * levelBuks + numSkipBkt << " among " << atts * adarein_level * 2 * levelBuks
		 << " buckets.\n";
	cout << "Skip attribute:";
	_for(i, 0, atts)
	{
		_for(j, 0, adarein_level)
		{
			if (skippedW[i][j])
			{
				cout << " a" << i << "w" << j;
			}
		}
		cout << ";  ";
	}
	cout << "\n";
#endif

	// <AttributeId, widthId, numPredicateNotSkipped>
	auto cmp = [&](const auto &a, const auto &b)
	{
		return get<2>(a) < get<2>(b);
	};
	vector<tuple<int, int, int>> attrLayerLoad;
	_for(ai, 0, atts)
	{
		_for(wj, 0, adarein_level)
		{
			if (!skippedW[ai][wj])
			{
				int numPredicateNotSkipped = 0;
				__for(bk, beBucketW[ai][wj].first.first, beBucketW[ai][wj].first.second)
				{
					numPredicateNotSkipped += dataW[ai][wj][0][bk].size();
				}
				__for(bk, beBucketW[ai][wj].second.second, beBucketW[ai][wj].second.first)
				{
					numPredicateNotSkipped += dataW[ai][wj][1][bk].size();
				}
				attrLayerLoad.emplace_back(ai, wj, numPredicateNotSkipped >> 1);
			}
		}
	}

	vector<int> threadWorkload(parallelDegree, 0);
	sort(attrLayerLoad.begin(), attrLayerLoad.end(),
		 [](const auto &a, const auto &b)
		 { return get<2>(a) > get<2>(b); });
	for (int li = 0; li < attrLayerLoad.size(); li++)
	{
		int minThreadNo = 0;
		_for(ti, 1, parallelDegree)
		{
			if (threadWorkload[ti] < threadWorkload[minThreadNo])
			{
				minThreadNo = ti;
			}
		}
		threadTaskSet[minThreadNo].emplace_back(get<0>(attrLayerLoad[li]), get<1>(attrLayerLoad[li]));
		threadWorkload[minThreadNo] += get<2>(attrLayerLoad[li]);
	}
	cout << "\nThreadNo Workload\n";
	_for(i, 0, parallelDegree)
	{
		cout << "T" << i << "\t" << threadWorkload[i] << "\n";
	}
	cout << "\n";

	_for(ti, 0, parallelDegree)
	{
		sort(threadTaskSet[ti].begin(), threadTaskSet[ti].end()); // Increasing by ai and wj
	}
}

void AdaRein::parallel2_approx_match_sss_c_w(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{

	vector<future<bitset<subs>>> threadResult;
	int tId = 0;
	for (int tId = 0; tId < parallelDegree; tId++)
	{
		threadResult.emplace_back(threadPool.enqueue([this, &pub, tId]
													 {
			Pub pub_c; // ��ά�����԰����Ժ�����ʱ����Ҫ����һ�� copy
			pub_c.pairs.resize(pub.pairs.size());
			for (int pi = 0; pi < pub.pairs.size(); pi++) {
				pub_c.pairs[pub.pairs[pi].att].value = pub.pairs[pi].value;
			}
			bitset<subs> b; //=new bitset<subs>;
			for (int taskId = 0; taskId < threadTaskSet[tId].size(); taskId++) {
				int ai = threadTaskSet[tId][taskId].first, wj = threadTaskSet[tId][taskId].second;
				int value = pub_c.pairs[ai].value, buck = value / levelBuckStep;
				for (auto cmb : dataW[ai][wj][0][buck])
					if (cmb.val > value)
						b[cmb.subID] = true;
				for (int j = max(buck + 1, beBucketW[ai][wj].first.first);
					j <= beBucketW[ai][wj].first.second; j++) // ��HEMϵ�е���Ʋ�ͬ, ����ȡ������
					for (auto&& k : dataW[ai][wj][0][j])
						b[k.subID] = true;

				for (auto cmb : dataW[ai][wj][1][buck])
					if (cmb.val < value)
						b[cmb.subID] = true;
				for (int j = min(buck - 1, beBucketW[ai][wj].second.first);
					j >= beBucketW[ai][wj].second.second; j--)
					for (auto&& k : dataW[ai][wj][1][j])
						b[k.subID] = true;
			}
			return b; }));
	}

	// �����滻Ϊ1��λ����
	bitset<subs> bits;
	if (pub.size < atts)
	{
		vector<bool> attExist(atts, false);
		for (const auto item : pub.pairs)
			attExist[item.att] = true;
		for (int ai = 0; ai < atts; ai++)
			if (!attExist[ai])
			{
				for (int wi = 0; wi < adarein_level; wi++)
				{
					if (skippedW[ai][wi])
						continue;
					for (int j = beBucketW[ai][wi].second.first; j >= beBucketW[ai][wi].second.second; j--)
						for (auto &&k : dataW[ai][wi][1][j])
							bits[k.subID] = true;
				}
			}
	}
	for (int i = 0; i < threadResult.size(); i++)
		bits |= threadResult[i].get();
	matchSubs = subs - bits.count();
}

void AdaRein::insert_dss_w(IntervalSub sub)
{
	int levelId = -1;
	Combo c;
	c.subID = sub.id;
	for (const auto &cnt : sub.constraints)
	{
		levelId = (cnt.highValue - cnt.lowValue) / widthStep;
		attsWidthPredicate[cnt.att][levelId]++; // Add this line to count the number of predicates defined in each attribute
		c.val = cnt.lowValue;
		dataW[cnt.att][levelId][0][c.val / levelBuckStep].push_back(c);
		c.val = cnt.highValue;
		dataW[cnt.att][levelId][1][c.val / levelBuckStep].push_back(c);
	}
}

void AdaRein::dynamic_succession_selection_width(double falsePositive, const vector<IntervalSub> &subList)
{
	calMaxSkipPredicate(falsePositive, subList);
}

void AdaRein::approx_match_dss_w(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	bitset<subs> bits;
	vector<bool> attExist(atts, false);
	int numSkipPredicate = 0, att, value, buck;
	for (auto &&iPair : pub.pairs)
	{
		att = iPair.att, value = iPair.value, buck = value / levelBuckStep;
		attExist[att] = true;

		_for(wi, 0, adarein_level)
		{
			if (numSkipPredicate + attsWidthPredicate[att][wi] <= maxSkipPredicate)
			{
				numSkipPredicate += attsWidthPredicate[att][wi];
#ifdef DEBUG
				numSkipAttsInTotal += 1;
#endif
				continue;
			}

			for (const auto &cmb : dataW[att][wi][0][buck])
				if (cmb.val > value)
					bits[cmb.subID] = true;

			for (int j = levelBuks - 1; j > buck; j--)
			{
				if (numSkipPredicate + (dataW[att][wi][0][j].size() >> 1) <= maxSkipPredicate)
				{
					numSkipPredicate += (dataW[att][wi][0][j].size() >> 1);
#ifdef DEBUG
					numSkipBuckInTotal++;
#endif
				}
				else
				{
					for (auto &&k : dataW[att][wi][0][j])
						bits[k.subID] = true;
				}
			}

			for (auto cmb : dataW[att][wi][1][buck])
				if (cmb.val < value)
					bits[cmb.subID] = true;

			for (int j = 0; j < buck; j++)
			{
				if (numSkipPredicate + (dataW[att][wi][1][j].size() >> 1) <= maxSkipPredicate)
				{
					numSkipPredicate += (dataW[att][wi][1][j].size() >> 1);
#ifdef DEBUG
					numSkipBuckInTotal++;
#endif
				}
				else
				{
					for (auto &&k : dataW[att][wi][1][j])
						bits[k.subID] = true;
				}
			}
		} // width index
	}	  // attribute index

	for (int i = 0; i < atts; i++)
		if (!attExist[i])
		{
			for (int wi = 0; wi < adarein_level; wi++)
			{
				if (numSkipPredicate + (attsWidthPredicate[i][wi] >> 1) <= maxSkipPredicate)
				{
					numSkipPredicate += (attsWidthPredicate[i][wi] >> 1);
#ifdef DEBUG
					numSkipAttsInTotal += 1;
#endif
					continue;
				}
				for (int j = 0; j < buks; j++)
				{
					if (numSkipPredicate + (dataW[i][wi][1][j].size() >> 1) <= maxSkipPredicate)
					{
						numSkipPredicate += (dataW[i][wi][1][j].size() >> 1);
#ifdef DEBUG
						numSkipBuckInTotal++;
#endif
					}
					else
					{
						for (auto &&k : dataW[i][wi][1][j])
							bits[k.subID] = true;
					}
				}
			}
		}

#ifdef DEBUG
	numSkipPredicateInTotal += numSkipPredicate;
#endif
	matchSubs = subs - bits.count();
}

void AdaRein::insert_dss_b(IntervalSub sub)
{
	Combo c;
	c.subID = sub.id;
	for (int i = 0; i < sub.size; i++)
	{
		IntervalCnt cnt = sub.constraints[i];
		attsPredicate[cnt.att]++; // Add this line to count the number of predicates defined in each attribute
		c.val = cnt.lowValue;
		data[0][cnt.att][c.val / buckStep].push_back(c);
		c.val = cnt.highValue;
		data[1][cnt.att][c.val / buckStep].push_back(c);
	}
	numSub++;
}

void AdaRein::dynamic_succession_selection_backward(double falsePositive, const vector<IntervalSub> &subList)
{
	calMaxSkipPredicate(falsePositive, subList);
}

void AdaRein::approx_match_dss_b(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
	bitset<subs> bits;
	vector<bool> attExist(atts, false);
	int numSkipPredicate = 0, att, value, buck;
	for (auto &&iPair : pub.pairs)
	{
		att = iPair.att;
		attExist[att] = true;
		if (numSkipPredicate + attsPredicate[att] <= maxSkipPredicate)
		{
			numSkipPredicate += attsPredicate[att];
#ifdef DEBUG
			numSkipAttsInTotal += 1;
#endif
			continue;
		}

		value = iPair.value, buck = value / buckStep;
		if (numSkipPredicate + (data[0][att][buck].size() >> 1) <= maxSkipPredicate)
		{
			numSkipPredicate += (data[0][att][buck].size() >> 1);
#ifdef DEBUG
			numSkipBuckInTotal++;
#endif
		}
		else
		{
			for (const auto &cb : data[0][att][buck])
			{
				if (cb.val > value)
					bits[cb.subID] = true;
			}
		}

		for (int j = buck + 1; j < buks; j++)
		{
			if (numSkipPredicate + (data[0][att][j].size() >> 1) <= maxSkipPredicate)
			{
				numSkipPredicate += (data[0][att][j].size() >> 1);
#ifdef DEBUG
				numSkipBuckInTotal++;
#endif
			}
			else
			{
				for (const auto &cb : data[0][att][j])
					bits[cb.subID] = true;
			}
		}

		if (numSkipPredicate + (data[1][att][buck].size() >> 1) <= maxSkipPredicate)
		{
			numSkipPredicate += (data[1][att][buck].size() >> 1);
#ifdef DEBUG
			numSkipBuckInTotal++;
#endif
		}
		else
		{
			for (const auto &cb : data[1][att][buck])
				if (cb.val < value)
					bits[cb.subID] = true;
		}

		for (int j = buck - 1; j >= 0; j--)
		{
			if (numSkipPredicate + (data[1][att][j].size() >> 1) <= maxSkipPredicate)
			{
				numSkipPredicate += (data[1][att][j].size() >> 1);
#ifdef DEBUG
				numSkipBuckInTotal++;
#endif
			}
			else
			{
				for (const auto &cb : data[1][att][j])
					bits[cb.subID] = true;
			}
		}
	}

	// �����滻Ϊ1��λ����
	for (int i = 0; i < atts; i++)
		if (!attExist[i])
		{
			if (numSkipPredicate + attsPredicate[i] <= maxSkipPredicate)
			{
				numSkipPredicate += attsPredicate[i];
#ifdef DEBUG
				numSkipAttsInTotal += 1;
#endif
				continue;
			}
			for (int j = 0; j < buks; j++)
			{
				if (numSkipPredicate + data[1][i][j].size() <= maxSkipPredicate)
				{
					numSkipPredicate += (data[1][i][j].size() >> 1);
#ifdef DEBUG
					numSkipBuckInTotal++;
#endif
				}
				else
				{
					for (const auto &cb : data[1][i][j])
						bits[cb.subID] = true;
				}
			}
		}

#ifdef DEBUG
	numSkipPredicateInTotal += numSkipPredicate;
#endif
	matchSubs = subs - bits.count();
}

void AdaRein::approx_match_dss_b_w(const Pub &pub, int &matchSubs, const vector<IntervalSub> &subList)
{
}

void AdaRein::run_AdaRein_SSS_C_W_consumer(const intervalGenerator &gen)
{

	vector<double> insertTimeList;
	vector<double> deleteTimeList;
	vector<double> matchTimeList_ad2;
	vector<double> matchTimeList_rein;

	vector<vector<double>> matchSubList_ad2;
	vector<vector<double>> realMatchSubList;
	vector<double> falsePositiveRateVec;

	vector<double> matchSubList_rein;
	vector<double> aveAdareinWindowLatencySum;
	vector<double> aveReinWindowLatencySum;

	// insert
	for (int i = 0; i < subs; i++)
	{
		// Timer insertStart;

		insert_sss_c_w(gen.subList[i]); // Insert sub[i] into data structure.

		// int64_t insertTime = insertStart.elapsed_nano(); // Record inserting time in nanosecond.
		// insertTimeList.push_back((double)insertTime / 1000000);
	}
	cout << "Insertion Finishes.\n";

	double initTime;
	Timer initStart;
	static_succession_selection_crossed_width(falsePositiveRate, gen.subList);
	initTime = (double)initStart.elapsed_nano() / 1000000.0; // 输出微秒
	cout << "AdaRein_SSS_C_W Skipping Task Finishes.\n";

	double lastWindowFalsePositiveRate = falsePositiveRate;
	double lastWindowLatencySum = 0.0;
	double adareinWindowLatencySum = 0.0;

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
	int32_t partition = RdKafka::Topic::PARTITION_UA; // Ϊ�β����ã�����Consumer����ֻ��д0�������޷��Զ��𣿣���
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
	// ofstream file("/home/k8smaster/dypKafka01/dypKafka/HEM/AdaRein_SSS_C_W_kafka.txt");
	// FILE* fp = NULL;
	//fp = fopen("percisionRates.txt", "w");

	falsePositiveRateVec.push_back(falsePositiveRate);
	while (true)
	{
		adareinWindowLatencySum = 0.0;
		windowEventNum = 0;
		matchSubList_ad2.push_back(vector<double>());
		realMatchSubList.push_back(vector<double>());
		double percisionRates = 0.0;

		while (windowEventNum < windowSize)
		{
			std::string msg_pubstr;
			double timestamp;
			int64_t now_time;
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
				double now = (tn.tv_sec * 1000000000 + tn.tv_nsec) / 1000000;
				timestamp = (now - (double)msg->timestamp().timestamp); // msg->timestamp(): millsecond
				// cout << "now: " << (unsigned long long)now << ", tstamp: " << msg->timestamp().timestamp << ", diff: " << timestamp << "\n";
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
			// cout << "receive event values: " << pub.pairs.size() << "\n";
			adareinWindowLatencySum += timestamp; // send waiting

			Timer matchStart;
			bitset<subs> bits;
			for (int att = 0; att < pub.pairs.size(); att++)
			{
				int value = pub.pairs[att].value, buck = value / levelBuckStep;
				_for(wi, 0, adarein_level)
				{
					if (skippedW[att][wi])
						continue;

					for (auto cmb : dataW[att][wi][0][buck])
						if (cmb.val > value)
							bits[cmb.subID] = true;
					for (int j = max(buck + 1, beBucketW[att][wi].first.first);
						 j <= beBucketW[att][wi].first.second; j++)
						for (auto &&k : dataW[att][wi][0][j])
							bits[k.subID] = true;

					for (auto cmb : dataW[att][wi][1][buck])
						if (cmb.val < value)
							bits[cmb.subID] = true;
					for (int j = min(buck - 1, beBucketW[att][wi].second.first);
						 j >= beBucketW[att][wi].second.second; j--)
						for (auto &&k : dataW[att][wi][1][j])
							bits[k.subID] = true;
				}
			}
			matchSubs = subs - bits.count();
			matchTime = (double)matchStart.elapsed_nano() / 1000000; // matchtime： 毫秒
			adareinWindowLatencySum += matchTime;
			matchSubList_ad2[matchSubList_ad2.size() - 1].push_back(matchSubs);
			matchTimeList_ad2.push_back(timestamp + matchTime);
			// cout << "WindowNo " << windowNo << ": ada2 match time = " << matchTime << ", matchSubs= " << matchSubs << "; \n";
			windowEventNum++;

			// //check compute realfalserate
			// {
			// 	int realMatchNum = 0;
			// 	for (int i = 0; i < gen.subList.size(); i++)
			// 	{
			// 		bool matchFlag = true;
			// 		for (int j = 0; matchFlag && j < gen.subList[i].size; j++)
			// 		{
			// 			IntervalCnt cnt = gen.subList[i].constraints[j];
			// 			bool flag = false;
			// 			for (int k = 0; k < pub.pairs.size(); k++)
			// 				if (pub.pairs[k].att == cnt.att && pub.pairs[k].value >= cnt.lowValue &&
			// 					pub.pairs[k].value <= cnt.highValue)
			// 					flag = true;
			// 			if (!flag)
			// 				matchFlag = false;
			// 		}
			// 		if (matchFlag)
			// 			++realMatchNum;
			// 	}
			//     //if (realMatchSubList[i][j] > 0)
			// 	percisionRates += (1.0 - (double)realMatchNum / (double)windowSize); //为什么除以windowsize？
			// 	realMatchSubList[realMatchSubList.size() - 1].push_back(realMatchNum);
			// 		cout << "RealMatchNum: " << realMatchNum << " 查全率: " << ((matchSubs == 0) ? 1 : ((double)(realMatchNum) / (double)matchSubs)) << " 错误率: " <<(double)(matchSubs - realMatchNum) / (double)gen.subList.size() * 100.0 << " %\n";

					
			// 		// if (file.is_open())
			// 	    // {
			// 		// file << "RealMatchNum: " << realMatchNum << " 查全率: " << ((matchSubs == 0) ? 1 : ((double)(realMatchNum) / (double)matchSubs)) <<
			// 		// " 错误率: " <<(double)(matchSubs - realMatchNum) / (double)gen.subList.size() * 100.0  << endl;
			// 		// //file.close();
			// 	    // }
			// }
		} // 窗口结束
        
		// fprintf(fp, "%f\n", percisionRates / (double)windowSize);

		adareinWindowLatencySum = adareinWindowLatencySum / windowSize;
		// aveAdareinWindowLatencySum.push_back(adareinWindowLatencySum);
		// aveReinWindowLatencySum.push_back(reinWindowLatencySum);

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
		falsePositiveRateVec.push_back(nextWindowFPR);
		cout << "windowNo: " << windowNo << ": lastWinLat= " << lastWindowLatencySum
			 << ", avgAdaReinWindowLatency= " << adareinWindowLatencySum
			 << "\n"
			 << "lastWindowFalsePositiveRate= " << lastWindowFalsePositiveRate
			 << ", nextWindowFPR= " << nextWindowFPR << "\n";
		lastWindowFalsePositiveRate = nextWindowFPR;
		lastWindowLatencySum = adareinWindowLatencySum;
		windowNo++;
		if (windowNo == 35)
			break;
	}

	consumer->stop(topic, partition);
	consumer->poll(0);
	delete topic;
	delete consumer;
	// fclose(fp);

	// #ifdef DEBUG
	// 	cout << "falseMatchNum= " << falseAvgMatchNum << ", realFalsePositiveRate= "
	// 		 << 1 - realMatchNum / falseAvgMatchNum << ", matchTime= "
	// 		 << Util::Double2String(Util::Average(matchTimeList)) << " ms\n";
	// #endif
	// 没有输出，延迟，论文里面画的是 second-eventLatency 图
	string outputFileName = "AdaRein_SSS_C_W_kafka.txt";
	string content = expID + " memory= " + Util::Int2String(calMemory_sss_c_w()) + " MB " + " AvgInsertTime= " + Util::Double2String(Util::Average(insertTimeList)) + " ms InitTime= " + Util::Double2String(initTime) + " ms AvgConstructionTime= " +
					 Util::Double2String(Util::Average(insertTimeList) + initTime / subs) + " ms AvgDeleteTime= " + Util::Double2String(Util::Average(deleteTimeList)) + " ms AvgMatchTime= " + Util::Double2String(Util::Average(matchTimeList_ad2)) + " ms level= " + Util::Int2String(adarein_level) + " maxSkipPre= " + Util::Int2String(maxSkipPredicate) + " fPR= " + Util::Double2String(falsePositiveRate) + ", numSub= " + Util::Int2String(subs) + " subSize= " + Util::Int2String(cons) + " numPub= " + Util::Int2String(pubs) + " pubSize= " + Util::Int2String(m) + " attTypes= " + Util::Int2String(atts) + " attGroup= " + Util::Int2String(attrGroup) + " attNumType= " + Util::Int2String(attNumType) + " valDom= " + Util::Double2String(valDom);

	content.append("\nexpectFalsePositiveRate= [\n");
	for (int i = 0; i < falsePositiveRateVec.size(); i++)
	{
		content.append(to_string(falsePositiveRateVec[i]));
		if (i < falsePositiveRateVec.size() - 1)
			content.append(", ");
		else
			content.append("]\n\n");
	}

	// content.append("\nrealFalsePositiveRate= [\n");
	// for (int i = 0; i < realMatchSubList.size(); i++)
	// {
	// 	double sumFPR = 0;
	// 	for (int j = 0; j < realMatchSubList[i].size(); j++)
	// 		if (realMatchSubList[i][j] > 0)
	// 			sumFPR += (double)abs(realMatchSubList[i][j] - matchSubList_ad2[i][j]) /(double)gen.subList.size();
	// 	sumFPR /= (double)realMatchSubList[i].size();
	// 	cout << sumFPR << ", ";
	// 	content.append(Util::Double2String(sumFPR));
	// 	if (i < realMatchSubList.size() - 1)
	// 		content.append(", ");
	// 	else
	// 		content.append("]\n");
	// }

	// content.append("\nMatchSubPerEvent= [\n");
	// for (int i = 0; i < matchSubList_ad2.size(); i++)
	// {
	// 	content.append("[");
	// 	for (int j = 0; j < windowSize; j++)
	// 	{
	// 		content.append(to_string(matchSubList_ad2[i][j]));
	// 		if (j < windowSize - 1)
	// 			content.append(", ");
	// 	}
	// 	content.append("]");
	// 	if (i < matchSubList_ad2.size() - 1)
	// 	{
	// 		content.append(",\n");
	// 	}
	// 	else
	// 	{
	// 		content.append("\n]\n\n");
	// 	}
	// }

	content.append("\nMatchTimePerWindow= [");
	for (int i = 0; i < matchTimeList_ad2.size() - 1; i++)
	{
		content.append(to_string(matchTimeList_ad2[i])).append(", ");
	}
	content.append(to_string(matchTimeList_ad2.back())).append("]\n\n");
	Util::WriteData2Begin(outputFileName.c_str(), content);

	outputFileName = "tmpData/AdaRein_SSS_C_W_kafka.txt";
	content = Util::Double2String(Util::Average(matchTimeList_ad2)) + ", ";
	Util::WriteData2End(outputFileName.c_str(), content);

	// outputFileName = "Rein_kafka.txt"
	// content = expID + " memory= " + Util::Int2String(calMemory_sss_c_w()) + " MB AvgMatchNum= " + Util::Double2String(Util::Average(matchSubList_rein)) + " AvgInsertTime= " + Util::Double2String(Util::Average(insertTimeList)) + " ms InitTime= " + Util::Double2String(initTime) + " ms AvgConstructionTime= " +
	// 		  Util::Double2String(Util::Average(insertTimeList) + initTime / subs) + " ms AvgDeleteTime= " + Util::Double2String(Util::Average(deleteTimeList)) + " ms AvgMatchTime= " + Util::Double2String(Util::Average(matchTimeList_rein)) + " ms level= " + Util::Int2String(adarein_level) + " maxSkipPre= " + Util::Int2String(maxSkipPredicate) + " fPR= " + Util::Double2String(falsePositiveRate) + " realfPR= " + Util::Double2String(1 - realMatchNum / falseAvgMatchNum) + " numSub= " + Util::Int2String(subs) + " subSize= " + Util::Int2String(cons) + " numPub= " + Util::Int2String(pubs) + " pubSize= " + Util::Int2String(m) + " attTypes= " + Util::Int2String(atts) + " attGroup= " + Util::Int2String(attrGroup) + " attNumType= " + Util::Int2String(attNumType) + " valDom= " + Util::Double2String(valDom);
	// content.append("\nMatchTimePerEvent= [");
	// for(int i=0;i<matchTimeList_rein.size()-1;i++){
	// 	content.append(to_string(matchTimeList_rein[i])).append(", ");
	// }
	// content.append(to_string(matchTimeList_rein.back())).append("]\n\n");
	// Util::WriteData2Begin(outputFileName.c_str(), content);

	outputFileName = "tmpData/Rein_kafka.txt";
	content = Util::Double2String(Util::Average(matchTimeList_rein)) + ", ";
	Util::WriteData2End(outputFileName.c_str(), content);
}

void AdaRein::run_AdaRein_ORI_consumer(const intervalGenerator &gen)
{

	Rein rein(OriginalRein);

	vector<double> insertTimeList;
	vector<double> deleteTimeList;
	vector<double> matchTimeList_adrein_ori;
	vector<vector<double>> matchSubList_adrein_ori;
	vector<double> falsePositiveRateVec;

	vector<double> aveAdareinWindowLatencySum;

	// insert
	for (int i = 0; i < subs; i++)
	{
		// Timer insertStart;

		insert(gen.subList[i]); // Insert sub[i] into data structure.
		rein.insert_backward_original(gen.subList[i]);

		// int64_t insertTime = insertStart.elapsed_nano(); // Record inserting time in nanosecond.
		// insertTimeList.push_back((double)insertTime / 1000000);
	}
	cout << "Insertion Finishes.\n";

	double initTime;
	Timer initStart;
	original_selection(falsePositiveRate, gen.subList);
	initTime = (double)initStart.elapsed_nano() / 1000000.0; // 输出微秒
	cout << "AdaRein_ori Skipping Task Finishes.\n";

	double lastWindowFalsePositiveRate = falsePositiveRate;
	double lastWindowLatencySum = 0.0;

	double adareinWindowLatencySum = 0.0;
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
	int32_t partition = RdKafka::Topic::PARTITION_UA; // Ϊ�β����ã�����Consumer����ֻ��д0�������޷��Զ��𣿣���
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
	ofstream file("/home/k8smaster/dypKafka01/dypKafka/HEM/AdaRein_ori_kafka.txt");
	while (true)
	{
		adareinWindowLatencySum = 0.0;
		reinWindowLatencySum = 0.0;
		windowEventNum = 0;
		matchSubList_adrein_ori.push_back(vector<double>());

		while (windowEventNum < windowSize)
		{
			std::string msg_pubstr;
			double timestamp;
			int64_t now_time;
			// bool if_read_data = true;
			/* * Consume messages */
			// Install a signal handler for clean shutdown.
			// signal(SIGINT, sigterm);
			if (run)
			{
				RdKafka::Message *msg;
				// now_time = time(0);
				// if_read_data = true;
				do
				{
					msg = consumer->consume(topic, partition, 1000000);
					msg_pubstr = msg_consume(msg, NULL);
					/* if ((time(0) - now_time)>10*60){
						if_read_data = false;
						break;
					} */
				} while (msg_pubstr == "errors");
				// if (msg_pubstr == "errors")
				// {
				// 	printf("consumer error\n");
				// 	exit(1);
				// }
				struct timespec tn;
				clock_gettime(0, &tn);
				double now = (tn.tv_sec * 1000000000 + tn.tv_nsec) / 1000000;
				timestamp = (now - (double)msg->timestamp().timestamp); // msg->timestamp(): millsecond
				// cout << "now: " << (unsigned long long)now << ", tstamp: " << msg->timestamp().timestamp << ", diff: " << timestamp << "\n";
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
			adareinWindowLatencySum += timestamp; // send waiting
			reinWindowLatencySum += timestamp;

			Timer matchStart;
			// bitset<subs> bits;
			vector<bool> bits(subs, false);
			vector<bool> attExist(atts, false);
			for (int i = 0; i < pub.size; i++)
			{
				int att = pub.pairs[i].att;
				attExist[att] = true;
				if (skipped[att])
					continue;
				int value = pub.pairs[i].value, buck = value / buckStep;
				for (int k = 0; k < data[0][att][buck].size(); k++)
					if (data[0][att][buck][k].val > value)
						bits[data[0][att][buck][k].subID] = true;
				for (int j = buck + 1; j < numBucket; j++)
					for (int k = 0; k < data[0][att][j].size(); k++)
						bits[data[0][att][j][k].subID] = true;

				for (int k = 0; k < data[1][att][buck].size(); k++)
					if (data[1][att][buck][k].val < value)
						bits[data[1][att][buck][k].subID] = true;
				for (int j = buck - 1; j >= 0; j--)
					for (int k = 0; k < data[1][att][j].size(); k++)
						bits[data[1][att][j][k].subID] = true;
			}

			for (int i = 0; i < atts; i++)
				if (!attExist[i] && !skipped[i])
					for (int j = 0; j < numBucket; j++)
						for (int k = 0; k < data[0][i][j].size(); k++)
							bits[data[0][i][j][k].subID] = true;

			for (int i = 0; i < gen.subList.size(); i++)
				if (!bits[i])
					++matchSubs;
			// matchSubs = subs - bits.count();
			matchTime = (double)matchStart.elapsed_nano() / 1000000; // matchtime： 毫秒
			adareinWindowLatencySum += matchTime;
			matchSubList_adrein_ori[matchSubList_adrein_ori.size() - 1].push_back(matchSubs);
			matchTimeList_adrein_ori.push_back(timestamp + matchTime);
			cout << "WindowNo " << windowNo << ": adrein_ori match time = " << matchTime << ", matchSubs= " << matchSubs << "; ";
			if (file.is_open())
			{
				file << "WindowNo " << windowNo << ": adrein_ori match time = " << matchTime << ", matchSubs= " << matchSubs << "; " << endl;
				// file.close();
			}
			// check
			//  {
			//  	int realMatchNum = 0;
			//  	for (int i = 0; i < gen.subList.size(); i++) {
			//  		bool matchFlag = true;
			//  		for (int j = 0; matchFlag && j < gen.subList[i].size; j++) {
			//  			IntervalCnt cnt = gen.subList[i].constraints[j];
			//  			bool flag = false;
			//  			for (int k = 0; k < pub.size; k++)
			//  				if (pub.pairs[k].att == cnt.att && pub.pairs[k].value >= cnt.lowValue &&
			//  					pub.pairs[k].value <= cnt.highValue)
			//  					flag = true;
			//  			if (!flag)
			//  				matchFlag = false;
			//  		}
			//  		if (matchFlag)
			//  			++realMatchNum;
			//  	}
			//  	cout << " realMatchNum: " << realMatchNum << " ";
			//  }

			// clock_t start_time, end_time;
			// matchStart.reset();
			// rein.match_backward_original(pub, matchSubs);
			// matchTime = (double)matchStart.elapsed_nano() / 1000000;
			// reinWindowLatencySum += matchTime;
			// matchSubList_rein.push_back(matchSubs);
			// matchTimeList_rein.push_back(timestamp + matchTime);
			//  cout << "rein match time = " << matchTime << "ms, matchSubs= " << matchSubs << endl;
			windowEventNum++;
		} // a window

		adareinWindowLatencySum = adareinWindowLatencySum / windowSize;
		// aveAdareinWindowLatencySum.push_back(adareinWindowLatencySum);
		reinWindowLatencySum = reinWindowLatencySum / windowSize;
		// aveReinWindowLatencySum.push_back(reinWindowLatencySum);

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
			original_selection(nextWindowFPR, gen.subList);

		cout << "windowNo: " << windowNo << ": lastWinLat= " << lastWindowLatencySum
			 << ", avgAdaReinWindowLatency= " << adareinWindowLatencySum
			 << ", avgReinWindowLatency= " << reinWindowLatencySum << "\n"
			 << "lastWindowFalsePositiveRate= " << lastWindowFalsePositiveRate
			 << ", nextWindowFPR= " << nextWindowFPR << "\n";

		if (file.is_open())
		{
			file << "windowNo: " << windowNo << ": lastWinLat= " << lastWindowLatencySum
				 << ", avgAdaReinWindowLatency= " << adareinWindowLatencySum
				 << ", avgReinWindowLatency= " << reinWindowLatencySum << "\n"
				 << "lastWindowFalsePositiveRate= " << lastWindowFalsePositiveRate
				 << ", nextWindowFPR= " << nextWindowFPR << "\n";
			// file.close();
		}

		lastWindowFalsePositiveRate = nextWindowFPR;
		lastWindowLatencySum = adareinWindowLatencySum;
		windowNo++;
		if (windowNo == 35)
			break;
	}

	consumer->stop(topic, partition);
	consumer->poll(0);
	delete topic;
	delete consumer;

	// #ifdef DEBUG
	// 	cout << "falseMatchNum= " << falseAvgMatchNum << ", realFalsePositiveRate= "
	// 		 << 1 - realMatchNum / falseAvgMatchNum << ", matchTime= "
	// 		 << Util::Double2String(Util::Average(matchTimeList)) << " ms\n";
	// #endif
	// 没有输出，延迟，论文里面画的是 second-eventLatency 图
	// output

	string outputFileName = "AdaRein_ori_kafka.txt";
	string content = expID + " memory= " + Util::Int2String(calMemory_sss_c_w()) + " MB, AvgInsertTime= " + Util::Double2String(Util::Average(insertTimeList)) + " ms InitTime= " + Util::Double2String(initTime) + " ms AvgConstructionTime= " +
					 Util::Double2String(Util::Average(insertTimeList) + initTime / subs) + " ms AvgDeleteTime= " + Util::Double2String(Util::Average(deleteTimeList)) + " ms AvgMatchTime= " + Util::Double2String(Util::Average(matchTimeList_adrein_ori)) + " ms level= " + Util::Int2String(adarein_level) + " maxSkipPre= " + Util::Int2String(maxSkipPredicate) + " fPR= " + Util::Double2String(falsePositiveRate) + " realfPR= numSub= " + Util::Int2String(subs) + " subSize= " + Util::Int2String(cons) + " numPub= " + Util::Int2String(pubs) + " pubSize= " + Util::Int2String(m) + " attTypes= " + Util::Int2String(atts) + " attGroup= " + Util::Int2String(attrGroup) + " attNumType= " + Util::Int2String(attNumType) + " valDom= " + Util::Double2String(valDom);

	content.append("\nrealFalsePositiveRate= [\n");
	for (int i = 0; i < falsePositiveRateVec.size(); i++)
	{
		content.append(to_string(falsePositiveRateVec[i]));
		if (i < falsePositiveRateVec.size() - 1)
			content.append(", ");
		else
			content.append("]\n\n");
	}

	content.append("\nMatchSubPerEvent= [\n");
	for (int i = 0; i < matchSubList_adrein_ori.size(); i++)
	{
		content.append("[");
		for (int j = 0; j < windowSize; j++)
		{
			content.append(to_string(matchSubList_adrein_ori[i][j]));
			if (j < windowSize - 1)
				content.append(", ");
		}
		content.append("]");
		if (i < matchSubList_adrein_ori.size() - 1)
			content.append(",\n");
		else
			content.append("\n]\n\n");
	}

	content.append("\nMatchTimePerEvent= [");
	for (int i = 0; i < matchTimeList_adrein_ori.size() - 1; i++)
	{
		content.append(to_string(matchTimeList_adrein_ori[i])).append(", ");
	}
	content.append(to_string(matchTimeList_adrein_ori.back())).append("]\n\n");
	Util::WriteData2Begin(outputFileName.c_str(), content);

	outputFileName = "tmpData/AdaRein__ori_kafka.txt";
	content = Util::Double2String(Util::Average(matchTimeList_adrein_ori)) + ", ";
	Util::WriteData2End(outputFileName.c_str(), content);

	// outputFileName = "Rein_kafka.txt";
	// content = expID + " memory= " + Util::Int2String(calMemory_sss_c_w()) + " MB AvgMatchNum= " + Util::Double2String(Util::Average(matchSubList_rein)) + " AvgInsertTime= " + Util::Double2String(Util::Average(insertTimeList)) + " ms InitTime= " + Util::Double2String(initTime) + " ms AvgConstructionTime= " +
	// 		  Util::Double2String(Util::Average(insertTimeList) + initTime / subs) + " ms AvgDeleteTime= " + Util::Double2String(Util::Average(deleteTimeList)) + " ms AvgMatchTime= " + Util::Double2String(Util::Average(matchTimeList_rein)) + " ms level= " + Util::Int2String(adarein_level) + " maxSkipPre= " + Util::Int2String(maxSkipPredicate) + " fPR= " + Util::Double2String(falsePositiveRate) + " realfPR= " + Util::Double2String(1 - realMatchNum / falseAvgMatchNum) + " numSub= " + Util::Int2String(subs) + " subSize= " + Util::Int2String(cons) + " numPub= " + Util::Int2String(pubs) + " pubSize= " + Util::Int2String(m) + " attTypes= " + Util::Int2String(atts) + " attGroup= " + Util::Int2String(attrGroup) + " attNumType= " + Util::Int2String(attNumType) + " valDom= " + Util::Double2String(valDom);
	// content.append("\nMatchTimePerEvent= [");
	// for(int i=0;i<matchTimeList_rein.size()-1;i++){
	// 	content.append(to_string(matchTimeList_rein[i])).append(", ");
	// }
	// content.append(to_string(matchTimeList_rein.back())).append("]\n\n");
	// Util::WriteData2Begin(outputFileName.c_str(), content);

	outputFileName = "tmpData/AdaRein_kafka.txt";
	Util::WriteData2End(outputFileName.c_str(), content);
}

void AdaRein::calMaxSkipPredicate(double falsePositive, const vector<IntervalSub> &subList)
{
	int numPredicate = 0;				 // ν������, numSkipPredicate �ѹ��˵�ν������
	double avgSubSize = 0, avgWidth = 0; // ƽ��ÿ�������ж��ٸ�ν��, ν�ʵ�ƽ������
	for (auto &&iSub : subList)
	{
		numPredicate += iSub.constraints.size();
		for (auto &&iCnt : iSub.constraints)
		{
			//++attsCounts[iCnt.att].count;
			avgWidth += iCnt.highValue - iCnt.lowValue;
		}
	}
	avgSubSize = (double)numPredicate / subList.size();
	avgWidth /= (double)numPredicate * valDom;

	// minPredicate �����Դ���
	// double minK = log(pow(avgWidth, avgSubSize) + falsePositive) / log(avgWidth);
	// inline auto valid = [&](double) {return (double)(numPredicate - numSkipPredicate) > minK * subs; };

	double falsePositiveRate_global = pow(avgWidth, avgSubSize) * subs / (1 - falsePositive) * falsePositive / subs;

	maxSkipPredicate = numPredicate - (avgSubSize - log(1 - falsePositive) / log(avgWidth)) * subs; // k2

#ifdef DEBUG
	cout << "k2= " << maxSkipPredicate << "\n";
	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) + falsePositiveRate_global) / log(avgWidth) *
						   subs; // ������Թ��˵�ν����, numSkipPredicate�����ֵ
	cout << "k3_global= " << maxSkipPredicate << "\n";
#endif

	maxSkipPredicate =
		numPredicate - log(pow(avgWidth, avgSubSize) / (1 - falsePositive)) / log(avgWidth) * subs;
	//	maxSkipPredicate *= 6;
	cout << "k3_local= " << maxSkipPredicate << "\n";
}

int AdaRein::calMemory()
{
	long long size = 0; // Byte
	_for(i, 0, atts) _for(j, 0, numBucket) size += sizeof(Combo) * (data[0][i][j].size() + data[1][i][j].size());
	size += sizeof(bool) * atts + sizeof(attAndCount) * atts;
	// cout << "attAndCount size = " << sizeof(attAndCount) << endl; // 8
	size = size / 1024 / 1024; // MB
	return (int)size;
}

int AdaRein::calMemory_sss_c_w()
{
	long long size = 0; // Byte
	_for(ai, 0, atts) _for(wi, 0, adarein_level) _for(bi, 0, levelBuks) size += sizeof(Combo) *
																				(dataW[ai][wi][0][bi].size() +
																				 dataW[ai][wi][1][bi].size());
	size += sizeof(bool) * atts + sizeof(attAndCount) * atts;
	// cout << "attAndCount size = " << sizeof(attAndCount) << endl; // 8
	size = size / 1024 / 1024; // MB
	return (int)size;
}

int AdaRein::calMemory_dss_w()
{
	long long size = 0; // Byte
	_for(ai, 0, atts)
	{
		_for(wi, 0, adarein_level)
		{
			_for(bi, 0, levelBuks)
			{
				size += sizeof(Combo) * (dataW[ai][wi][0][bi].size() + dataW[ai][wi][1][bi].size());
			}
		}
		size += sizeof(bool) * atts;
	}
	size += sizeof(int) * atts * adarein_level;
	// cout << "attAndCount size = " << sizeof(attAndCount) << endl; // 8
	size = size / 1024 / 1024; // MB
	return (int)size;
}
