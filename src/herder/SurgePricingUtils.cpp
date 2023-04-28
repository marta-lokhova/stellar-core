// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "herder/SurgePricingUtils.h"
#include "crypto/SecretKey.h"
#include "transactions/TransactionFrameBase.h"
#include "util/Logging.h"
#include "util/numeric128.h"
#include "util/types.h"
#include <numeric>

namespace stellar
{

Resource
mutliplyByDouble(Resource const& res, double m)
{
    auto newRes = res;
    for (auto& resource : newRes.mResources)
    {
        auto tempResultDbl = resource * m;
        // Multiple each resource dimension by the rate
        releaseAssertOrThrow(tempResultDbl >= 0.0);
        releaseAssertOrThrow(isRepresentableAsInt64(tempResultDbl));
        resource = static_cast<int64_t>(tempResultDbl);
    }

    return newRes;
}

Resource
bigDivideOrThrow(Resource const& res, int64_t B, int64_t C, Rounding rounding)
{
    auto newRes = res;
    for (auto& resource : newRes.mResources)
    {
        resource = bigDivideOrThrow(resource, B, C, rounding);
    }

    return newRes;
}

bool
operator==(Resource const& lhs, Resource const& rhs)
{
    return lhs.mResources == rhs.mResources;
}

Resource
operator+(Resource const& lhs, Resource const& rhs)
{
    releaseAssert(lhs.mResources.size() == rhs.mResources.size());
    std::vector<int64_t> res;
    for (size_t i = 0; i < lhs.mResources.size(); i++)
    {
        releaseAssert(UINT32_MAX - lhs.mResources[i] >= rhs.mResources[i]);
        res.push_back(lhs.mResources[i] + rhs.mResources[i]);
    }
    return Resource(res);
}

Resource
operator-(Resource const& lhs, Resource const& rhs)
{
    releaseAssert(lhs.mResources.size() == rhs.mResources.size());
    std::vector<int64_t> res;
    for (size_t i = 0; i < lhs.mResources.size(); i++)
    {
        releaseAssert(lhs.mResources[i] >= rhs.mResources[i]);
        res.push_back(lhs.mResources[i] - rhs.mResources[i]);
    }
    return Resource(res);
}

bool
operator<=(Resource const& lhs, Resource const& rhs)
{
    releaseAssert(lhs.mResources.size() == rhs.mResources.size());
    return lhs.mResources <= rhs.mResources;
}

Resource&
Resource::operator+=(Resource const& other)
{
    releaseAssert(mResources.size() == other.mResources.size());
    for (size_t i = 0; i < mResources.size(); i++)
    {
        releaseAssert(INT64_MAX - mResources[i] >= other.mResources[i]);
        mResources[i] += other.mResources[i];
    }
    return *this;
}

Resource&
Resource::operator-=(Resource const& other)
{
    releaseAssert(mResources.size() == other.mResources.size());
    for (size_t i = 0; i < mResources.size(); i++)
    {
        releaseAssert(mResources[i] >= other.mResources[i]);
        mResources[i] -= other.mResources[i];
    }
    return *this;
}

bool
Resource::operator<(Resource const& other) const
{
    releaseAssert(mResources.size() == other.mResources.size());
    // All of current rsources must be less than other resources
    return mResources < other.mResources;
}

bool
Resource::operator>(Resource const& other) const
{
    releaseAssert(mResources.size() == other.mResources.size());
    // All of current rsources must be greater than other resources
    return mResources > other.mResources;
}

namespace
{

int
feeRate3WayCompare(TransactionFrameBase const& l, TransactionFrameBase const& r)
{
    return stellar::feeRate3WayCompare(l.getFeeBid(), l.getNumOperations(),
                                       r.getFeeBid(), r.getNumOperations());
}

} // namespace

int
feeRate3WayCompare(int64_t lFeeBid, uint32_t lNbOps, int64_t rFeeBid,
                   uint32_t rNbOps)
{
    // Let f1, f2 be the two fee bids, and let n1, n2 be the two
    // operation counts. We want to calculate the boolean comparison
    // "f1 / n1 < f2 / n2" but, since these are uint128s, we want to
    // avoid the truncating division or use of floating point.
    //
    // Therefore we multiply both sides by n1 * n2, and cancel:
    //
    //               f1 / n1 < f2 / n2
    //  == f1 * n1 * n2 / n1 < f2 * n1 * n2 / n2
    //  == f1 *      n2      < f2 * n1
    auto v1 = bigMultiply(lFeeBid, rNbOps);
    auto v2 = bigMultiply(rFeeBid, lNbOps);
    if (v1 < v2)
    {
        return -1;
    }
    else if (v1 > v2)
    {
        return 1;
    }
    return 0;
}

int64_t
computeBetterFee(TransactionFrameBase const& tx, int64_t refFeeBid,
                 uint32_t refNbOps)
{
    constexpr auto m = std::numeric_limits<int64_t>::max();

    int64 minFee = m;
    int64 v;
    if (bigDivide(v, refFeeBid, tx.getNumOperations(), refNbOps,
                  Rounding::ROUND_DOWN) &&
        v < m)
    {
        minFee = v + 1;
    }
    return minFee;
}

SurgePricingPriorityQueue::TxStackComparator::TxStackComparator(bool isGreater,
                                                                size_t seed)
    : mIsGreater(isGreater), mSeed(seed)
{
}

bool
SurgePricingPriorityQueue::TxStackComparator::operator()(
    TxStackPtr const& txStack1, TxStackPtr const& txStack2) const
{
    return txStackLessThan(*txStack1, *txStack2) ^ mIsGreater;
}

bool
SurgePricingPriorityQueue::TxStackComparator::compareFeeOnly(
    TransactionFrameBase const& tx1, TransactionFrameBase const& tx2) const
{
    return compareFeeOnly(tx1.getFeeBid(), tx1.getNumOperations(),
                          tx2.getFeeBid(), tx2.getNumOperations());
}

bool
SurgePricingPriorityQueue::TxStackComparator::compareFeeOnly(
    int64_t tx1Bid, uint32_t tx1Ops, int64_t tx2Bid, uint32_t tx2Ops) const
{
    bool isLess = feeRate3WayCompare(tx1Bid, tx1Ops, tx2Bid, tx2Ops) < 0;
    return isLess ^ mIsGreater;
}

bool
SurgePricingPriorityQueue::TxStackComparator::isGreater() const
{
    return mIsGreater;
}

bool
SurgePricingPriorityQueue::TxStackComparator::txStackLessThan(
    TxStack const& txStack1, TxStack const& txStack2) const
{
    if (txStack1.empty())
    {
        return !txStack2.empty();
    }
    if (txStack2.empty())
    {
        return false;
    }
    return txLessThan(txStack1.getTopTx(), txStack2.getTopTx(), true);
}

// TODO: will need a separate comparator for Soroban
bool
SurgePricingPriorityQueue::TxStackComparator::txLessThan(
    TransactionFrameBaseConstPtr const& tx1,
    TransactionFrameBaseConstPtr const& tx2, bool breakTiesWithHash) const
{
    auto cmp3 = feeRate3WayCompare(*tx1, *tx2);

    if (cmp3 != 0 || !breakTiesWithHash)
    {
        return cmp3 < 0;
    }
    // break tie with pointer arithmetic
    auto lx = reinterpret_cast<size_t>(tx1.get()) ^ mSeed;
    auto rx = reinterpret_cast<size_t>(tx2.get()) ^ mSeed;
    return lx < rx;
}

SurgePricingPriorityQueue::SurgePricingPriorityQueue(
    bool isHighestPriority, std::shared_ptr<SurgePricingLaneConfig> settings,
    size_t comparisonSeed)
    : mComparator(isHighestPriority, comparisonSeed)
    , mLaneConfig(settings)
    , mLaneOpsLimits(mLaneConfig->getLaneLimits())
    , mLaneCurrentCount(mLaneOpsLimits.size(), Resource(0))
    , mTxStackSets(mLaneOpsLimits.size(), TxStackSet(mComparator))
{
    releaseAssert(!mLaneOpsLimits.empty());
}

std::vector<TransactionFrameBasePtr>
SurgePricingPriorityQueue::getMostTopTxsWithinLimits(
    std::vector<TxStackPtr> const& txStacks,
    std::vector<bool>& hadTxNotFittingLane)
{
    for (auto txStack : txStacks)
    {
        add(txStack);
    }
    std::vector<TransactionFrameBasePtr> txs;
    auto visitor = [&txs](TxStack const& txStack) {
        txs.push_back(txStack.getTopTx());
        return VisitTxStackResult::TX_PROCESSED;
    };
    std::vector<Resource> laneOpsLeftUntilLimit;
    popTopTxs(/* allowGaps */ true, visitor, laneOpsLeftUntilLimit,
              hadTxNotFittingLane);
    return txs;
}

void
SurgePricingPriorityQueue::visitTopTxs(
    std::vector<TxStackPtr> const& txStacks,
    std::function<VisitTxStackResult(TxStack const&)> const& visitor,
    std::vector<Resource>& laneOpsLeftUntilLimit)
{
    for (auto txStack : txStacks)
    {
        add(txStack);
    }
    std::vector<bool> hadTxNotFittingLane;
    popTopTxs(/* allowGaps */ false, visitor, laneOpsLeftUntilLimit,
              hadTxNotFittingLane);
}

void
SurgePricingPriorityQueue::add(TxStackPtr txStack)
{
    releaseAssert(txStack != nullptr);
    auto lane = mLaneConfig->getLane(*txStack->getTopTx());
    bool inserted = mTxStackSets[lane].insert(txStack).second;
    if (inserted)
    {
        mLaneCurrentCount[lane] += txStack->getResources();
    }
}

void
SurgePricingPriorityQueue::erase(TxStackPtr txStack)
{
    releaseAssert(txStack != nullptr);
    auto lane = mLaneConfig->getLane(*txStack->getTopTx());
    auto it = mTxStackSets[lane].find(txStack);
    if (it != mTxStackSets[lane].end())
    {
        erase(lane, it);
    }
}

void
SurgePricingPriorityQueue::erase(Iterator const& it)
{
    auto innerIt = it.getInnerIter();
    erase(innerIt.first, innerIt.second);
}

void
SurgePricingPriorityQueue::erase(
    size_t lane, SurgePricingPriorityQueue::TxStackSet::iterator iter)
{
    auto ops = (*iter)->getResources();
    mLaneCurrentCount[lane] -= ops;
    mTxStackSets[lane].erase(iter);
}

void
SurgePricingPriorityQueue::popTopTxs(
    bool allowGaps,
    std::function<VisitTxStackResult(TxStack const&)> const& visitor,
    std::vector<Resource>& laneOpsLeftUntilLimit,
    std::vector<bool>& hadTxNotFittingLane)
{
    laneOpsLeftUntilLimit = mLaneOpsLimits;
    hadTxNotFittingLane.assign(mLaneOpsLimits.size(), false);
    while (true)
    {
        // Sorted by fee rate, so the first iterator is the one with the highest
        auto currIt = getTop();
        bool gapSkipped = false;
        bool canPopSomeTx = true;

        // Try to find a lane in the top iterator that hasn't reached limit yet.
        while (!currIt.isEnd())
        {
            auto const& currTx = *(*currIt)->getTopTx();
            auto currOps = currTx.getNumResources();
            auto lane = mLaneConfig->getLane(currTx);
            if (currOps > laneOpsLeftUntilLimit[lane] ||
                currOps > laneOpsLeftUntilLimit[GENERIC_LANE])
            {
                if (allowGaps)
                {
                    // If gaps are allowed we just erase the iterator and
                    // continue in the main loop.
                    gapSkipped = true;
                    erase(currIt);
                    if (currOps > laneOpsLeftUntilLimit[lane])
                    {
                        hadTxNotFittingLane[lane] = true;
                    }
                    else
                    {
                        hadTxNotFittingLane[GENERIC_LANE] = true;
                    }
                    break;
                }
                else
                {
                    // If gaps are not allowed, but we only reach the limit for
                    // the 'limited' lane, then we still can find some
                    // transactions in the other lanes that would fit into
                    // 'generic' and their own lanes.
                    // We don't break the 'no gaps' invariant by doing so -
                    // every lane stops being iterated over as soon as
                    // non-fitting tx is found.
                    if (lane != GENERIC_LANE &&
                        currOps > laneOpsLeftUntilLimit[lane])
                    {
                        currIt.dropLane();
                    }
                    else
                    {
                        canPopSomeTx = false;
                        break;
                    }
                }
            }
            else
            {
                break;
            }
        }
        if (gapSkipped)
        {
            continue;
        }
        if (!canPopSomeTx || currIt.isEnd())
        {
            break;
        }
        // At this point, `currIt` points at the top transaction in the queue
        // (within the allowed lanes) that is still within operation limits, so
        // we can visit it and remove it from the queue.
        auto const& txStack = *currIt;
        auto visitRes = visitor(*txStack);
        auto ops = txStack->getTopTx()->getNumOperations();
        auto lane = mLaneConfig->getLane(*txStack->getTopTx());
        // Only account for operation counts when transaction was actually
        // processed by the visitor.
        if (visitRes == VisitTxStackResult::TX_PROCESSED)
        {
            // 'Generic' lane's limit is shared between all the transactions.
            laneOpsLeftUntilLimit[GENERIC_LANE] -= ops;
            if (lane != GENERIC_LANE)
            {
                laneOpsLeftUntilLimit[lane] -= ops;
            }
        }
        // Pop the tx from current iterator, unless the whole tx stack is
        // skipped.
        if (visitRes == VisitTxStackResult::TX_STACK_SKIPPED)
        {
            erase(currIt);
        }
        else
        {
            popTopTx(currIt);
        }
    }
}

// TODO: allow lower-priced smart txs to take place within the limits? is it
// fair?
std::pair<bool, int64_t>
SurgePricingPriorityQueue::canFitWithEviction(
    TransactionFrameBase const& tx, Resource txOpsDiscount,
    std::vector<std::pair<TxStackPtr, bool>>& txStacksToEvict) const
{
    // This only makes sense when the lowest fee rate tx is on top.
    releaseAssert(!mComparator.isGreater());

    auto lane = mLaneConfig->getLane(tx);
    if (tx.getNumResources() < txOpsDiscount)
    {
        throw std::invalid_argument(
            "Discount shouldn't be larger than transaction operations count");
    }
    Resource txNewResources = tx.getNumResources() - txOpsDiscount;
    if (txNewResources > mLaneOpsLimits[GENERIC_LANE] ||
        txNewResources > mLaneOpsLimits[lane])
    {
        // CLOG_WARNING(
        //     Herder,
        //     "Transaction fee lane has lower size than transaction ops: {} < "
        //     "{}. Node won't be able to accept all transactions.",
        //     txNewResources > mLaneOpsLimits[GENERIC_LANE]
        //         ? mLaneOpsLimits(GENERIC_LANE)
        //         : getOpsLimitsForLane(lane),
        //     txNewResources);
        return std::make_pair(false, 0ll);
    }
    Resource total = totalResources();
    Resource newTotalResources = total + txNewResources;
    Resource newLaneResources = mLaneCurrentCount[lane] + txNewResources;
    // To fit the eviction, tx has to both fit into 'generic' lane's limit
    // (shared among all the txs) and its own lane's limit.
    bool fitWithoutEviction =
        newTotalResources <= mLaneOpsLimits[GENERIC_LANE] &&
        newLaneResources <= mLaneOpsLimits[lane];
    // If there is enough space, return.
    if (fitWithoutEviction)
    {
        return std::make_pair(true, 0ll);
    }
    auto iter = getTop();

    auto neededTotalDontFit = [&](Resource const& l, Resource const& r) {
        if (l < r)
        {
            return Resource(std::vector<int64_t>(l.size(), 0));
        }
        return l - r;
    };

    Resource neededTotalOps =
        neededTotalDontFit(newTotalResources, mLaneOpsLimits[GENERIC_LANE]);
    Resource neededLaneOps =
        neededTotalDontFit(newLaneResources, mLaneOpsLimits[lane]);

    // The above checks should ensure there are some operations that need to be
    // evicted.
    // TODO: first check, is there a negative number?
    releaseAssert(!neededTotalOps.isZero() || !neededLaneOps.isZero());
    while (!neededTotalOps.isZero() || !neededLaneOps.isZero())
    {
        bool evictedDueToLaneLimit = false;
        while (!iter.isEnd())
        {
            auto const& evictTxStack = *iter;
            auto const& evictTx = *evictTxStack->getTopTx();
            auto evictLane = mLaneConfig->getLane(evictTx);
            auto evictOps = evictTxStack->getResources();
            bool canEvict = false;
            // Check if it makes sense to evict the current top tx stack.
            //
            // Simple cases: generic lane txs can evict anything; same lane txs
            // can evict each other; any transaction can be evicted if per-lane
            // limit hasn't been reached.
            if (lane == GENERIC_LANE || lane == evictLane || neededLaneOps <= 0)
            {
                canEvict = true;
            }
            else if (neededTotalOps <= neededLaneOps)
            {
                // When we need to evict more ops from the tx's lane than from
                // the generic lane, then we can only evict from tx's lane (any
                // other evictions are pointless).
                evictedDueToLaneLimit = true;
            }
            else
            {
                // In the final case `neededOps` is greater than `neededLaneOps`
                // and the difference can be evicted from any lane.
                releaseAssert(neededTotalOps > neededLaneOps);
                Resource nonLaneOpsToEvict = neededTotalOps - neededLaneOps;
                canEvict = evictOps <= nonLaneOpsToEvict;
            }
            if (!canEvict)
            {
                // If we couldn't evict a cheaper transaction from a lane, then
                // we shouldn't evict more expensive ones (even though some of
                // them could be evicted following the `canEvict` logic above).
                iter.dropLane();
            }
            else
            {
                break;
            }
        }
        // The preconditions are ensured above that we should be able to fit
        // the transaction by evicting some transactions.
        releaseAssert(!iter.isEnd());
        auto const& evictTxStack = *iter;
        auto const& evictTx = *evictTxStack->getTopTx();
        auto evictOps = evictTxStack->getResources();
        auto evictLane = mLaneConfig->getLane(evictTx);
        // Only support this for single tx stacks (eventually we should only
        // have such stacks and share this invariant across all the queue
        // operations).
        releaseAssert(evictOps == evictTx.getNumResources());

        // Evicted tx must have a strictly lower fee than the new tx.
        if (!mComparator.compareFeeOnly(evictTx, tx))
        {
            auto minFee = computeBetterFee(tx, evictTx.getFeeBid(),
                                           evictTx.getNumOperations());
            return std::make_pair(false, minFee);
        }
        // Ensure that this transaction is not from the same account.
        if (tx.getSourceID() == evictTx.getSourceID())
        {
            return std::make_pair(false, 0ll);
        }

        txStacksToEvict.emplace_back(evictTxStack, evictedDueToLaneLimit);
        neededTotalOps = neededTotalDontFit(neededTotalOps, evictOps);
        // Only reduce the needed lane ops when we evict from tx's lane or when
        // we're trying to fit a 'generic' lane tx (as it can evict any other
        // tx).
        if (lane == GENERIC_LANE || lane == evictLane)
        {
            neededLaneOps = neededTotalDontFit(neededLaneOps, evictOps);
        }

        if (neededTotalOps.isZero() && neededLaneOps.isZero())
        {
            return std::make_pair(true, 0ll);
        }

        iter.advance();
    }
    // The preconditions must guarantee that we find an evicted transaction set.
    releaseAssert(false);
}

SurgePricingPriorityQueue::Iterator
SurgePricingPriorityQueue::getTop() const
{
    std::vector<LaneIter> iters;
    for (size_t lane = 0; lane < mTxStackSets.size(); ++lane)
    {
        auto const& txStackSet = mTxStackSets[lane];
        if (txStackSet.empty())
        {
            continue;
        }
        iters.emplace_back(lane, txStackSet.begin());
    }
    return SurgePricingPriorityQueue::Iterator(*this, iters);
}

Resource
SurgePricingPriorityQueue::totalResources() const
{
    // There must be at least one lane.
    releaseAssert(!mLaneCurrentCount.empty());
    auto resourceCount = mLaneCurrentCount.begin()->mResources.size();
    Resource res(std::vector<int64_t>(resourceCount, 0));
    std::for_each(mLaneCurrentCount.begin(), mLaneCurrentCount.end(),
                  [&](auto const& laneCount) { res += laneCount; });
    return res;
}

Resource
SurgePricingPriorityQueue::laneResources(size_t lane) const
{
    releaseAssert(lane < mLaneCurrentCount.size());
    return mLaneCurrentCount[lane];
}

void
SurgePricingPriorityQueue::popTopTx(SurgePricingPriorityQueue::Iterator iter)
{
    auto txStack = *iter;
    erase(iter);
    txStack->popTopTx();
    if (!txStack->empty())
    {
        add(txStack);
    }
}

SurgePricingPriorityQueue::Iterator::Iterator(
    SurgePricingPriorityQueue const& parent, std::vector<LaneIter> const& iters)
    : mParent(parent), mIters(iters)
{
}

std::vector<SurgePricingPriorityQueue::LaneIter>::iterator
SurgePricingPriorityQueue::Iterator::getMutableInnerIter() const
{
    releaseAssert(!isEnd());
    auto best = mIters.begin();
    for (auto groupIt = std::next(mIters.begin()); groupIt != mIters.end();
         ++groupIt)
    {
        auto& [group, it] = *groupIt;
        if (mParent.mComparator(*it, *best->second))
        {
            best = groupIt;
        }
    }
    return best;
}

TxStackPtr
SurgePricingPriorityQueue::Iterator::operator*() const
{
    return *getMutableInnerIter()->second;
}

SurgePricingPriorityQueue::LaneIter
SurgePricingPriorityQueue::Iterator::getInnerIter() const
{
    return *getMutableInnerIter();
}

bool
SurgePricingPriorityQueue::Iterator::isEnd() const
{
    return mIters.empty();
}

void
SurgePricingPriorityQueue::Iterator::advance()
{
    auto it = getMutableInnerIter();
    ++it->second;
    if (it->second == mParent.mTxStackSets[it->first].end())
    {
        mIters.erase(it);
    }
}

void
SurgePricingPriorityQueue::Iterator::dropLane()
{
    mIters.erase(getMutableInnerIter());
}

// Difference not noticeable for DexLimitingLaneConfig users
DexLimitingLaneConfig::DexLimitingLaneConfig(
    Resource opsLimit, std::optional<Resource> dexOpsLimit)
{
    mLaneOpsLimits.push_back(opsLimit);
    if (dexOpsLimit)
    {
        mLaneOpsLimits.push_back(*dexOpsLimit);
    }
}

std::vector<Resource> const&
DexLimitingLaneConfig::getLaneLimits() const
{
    return mLaneOpsLimits;
}

void
DexLimitingLaneConfig::updateGenericLaneLimit(Resource const& limit)
{
    mLaneOpsLimits[0] = limit;
}

size_t
DexLimitingLaneConfig::getLane(TransactionFrameBase const& tx) const
{
    if (mLaneOpsLimits.size() > DexLimitingLaneConfig::DEX_LANE &&
        tx.hasDexOperations())
    {
        return DexLimitingLaneConfig::DEX_LANE;
    }
    else
    {
        return SurgePricingPriorityQueue::GENERIC_LANE;
    }
}
} // namespace stellar
