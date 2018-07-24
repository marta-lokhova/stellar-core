// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "VerifyMaybeReplayTxsWork.h"
#include "VerifyMaybeReplayTxsSnapWork.h"
#include "catchup/CatchupManager.h"
#include "historywork/Progress.h"
#include "ledger/LedgerManager.h"

namespace stellar
{
VerifyMaybeReplayTxsWork::VerifyMaybeReplayTxsWork(
    Application& app, WorkParent& parent, LedgerRange range,
    TmpDir const& downloadDir, bool apply,
    LedgerHeaderHistoryEntry& lastApplied)
    : BatchWork(app, parent,
                fmt::format("verify-apply-ledgers-{:08x}-{:08x}", range.first(),
                            range.last()))
    , mShouldApply(apply)
    , mRange(range)
    , mCheckpointRange(CheckpointRange{range, mApp.getHistoryManager()})
    , mNext(mCheckpointRange.first())
    , mDownloadDir(downloadDir)
    , mLastApplied(lastApplied)
{
}

VerifyMaybeReplayTxsWork::~VerifyMaybeReplayTxsWork()
{
    clearChildren();
}

std::string
VerifyMaybeReplayTxsWork::getStatus() const
{
    if (mState == WORK_RUNNING || mState == WORK_PENDING)
    {
        auto task = "verifying and replaying transactions";
        return fmtProgress(mApp, task, mRange.first(), mRange.last(), mNext);
    }
    return Work::getStatus();
}

void
VerifyMaybeReplayTxsWork::onReset()
{
    mLastApplied = mApp.getLedgerManager().getLastClosedLedgerHeader();
    BatchWork::onReset();
}

bool
VerifyMaybeReplayTxsWork::hasNext()
{
    return mNext <= mCheckpointRange.last();
}

void
VerifyMaybeReplayTxsWork::resetIter()
{
    mNext = mCheckpointRange.first();
}

std::shared_ptr<BatchableWork>
VerifyMaybeReplayTxsWork::yieldMoreWork()
{
    if (!hasNext())
    {
        throw std::runtime_error("Nothing to iterate over!");
    }

    auto snapshot = addWork<VerifyMaybeReplayTxsSnapWork>(
        mDownloadDir, mNext, mRange, mLastApplied, mShouldApply);

    mNext += mApp.getHistoryManager().getCheckpointFrequency();
    return snapshot;
}
}