// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "VerifyMaybeReplayTxsSnapWork.h"
#include "herder/LedgerCloseData.h"
#include "history/FileTransferInfo.h"
#include "history/HistoryManager.h"
#include "historywork/GetAndUnzipRemoteFileWork.h"
#include "ledger/LedgerManager.h"
#include "util/Logging.h"
#include <catchup/ApplyLedgerChainWork.h>

namespace stellar
{
VerifyMaybeReplayTxsSnapWork::VerifyMaybeReplayTxsSnapWork(
    Application& app, WorkParent& parent, TmpDir const& downloadDir,
    uint32_t checkpoint, LedgerRange const& range,
    LedgerHeaderHistoryEntry& lastApplied, bool shouldApply)
    : BatchableWork(app, parent,
                    "verify-apply-tx-" + std::to_string(checkpoint))
    , mDownloadDir(downloadDir)
    , mCheckpoint(checkpoint)
    , mRange(range)
    , mLastApplied(lastApplied)
    , mShouldApply(shouldApply)
    , mDownloadTxsStart(app.getMetrics().NewMeter(
          {"history", "download-transactions", "start"}, "event"))
    , mDownloadTxsSuccess(app.getMetrics().NewMeter(
          {"history", "download-transactions", "success"}, "event"))
    , mDownloadTxsFailure(app.getMetrics().NewMeter(
          {"history", "download-transactions", "failure"}, "event"))
{
}

VerifyMaybeReplayTxsSnapWork::~VerifyMaybeReplayTxsSnapWork()
{
    clearChildren();
}

std::string
VerifyMaybeReplayTxsSnapWork::getStatus() const
{
    if (mState == WORK_PENDING || mState == WORK_RUNNING)
    {
        if (mVerifyApplyWork)
        {
            return mVerifyApplyWork->getStatus();
        }
    }

    return BatchableWork::getStatus();
}

void
VerifyMaybeReplayTxsSnapWork::onReset()
{
    mVerifyApplyWork.reset();
}

bool
VerifyMaybeReplayTxsSnapWork::verifyAndMaybeReplayTxs()
{
    if (mVerifyApplyWork)
    {
        assert(mVerifyApplyWork->getState() == Work::WORK_SUCCESS);
        return true;
    }

    auto applying = mShouldApply ? "and applying" : "";
    CLOG(INFO, "History") << "Downloading, verifying " << applying
                          << " transactions for checkpoint " << mCheckpoint;
    FileTransferInfo ft{mDownloadDir, HISTORY_FILE_TYPE_TRANSACTIONS,
                        mCheckpoint};
    mDownloadTxsStart.Mark();
    mVerifyApplyWork = addWork<ApplyLedgerChainWork>(mDownloadDir, mCheckpoint,
                                                     mRange, mShouldApply);
    mVerifyApplyWork->addWork<GetAndUnzipRemoteFileWork>(ft);

    // VerifyMaybeReplayTxsSnapWork is instantly ready in two cases:
    // * This is the first checkpoint applied, rely on LCL
    // * Verification only is performed (checkpoints verification is
    // independent)
    auto first =
        mApp.getHistoryManager().checkpointContainingLedger(mRange.first());
    if (first == mCheckpoint || !mShouldApply)
    {
        mVerifyApplyWork->makeReady();
    }
    return false;
}

void
VerifyMaybeReplayTxsSnapWork::unblockWork(BatchableWorkResultData const& data)
{
    // Provide the following guarantee: if work is ready to be verified, it is
    // ready to be applied as well.
    // At this point we know that the checkpoint we depend on has finished
    // verifying *AND* applying, so we can proceed with verification and
    // application of the current checkpoint

    CLOG(DEBUG, "History")
        << "Transactions snapshot for checkpoint " << mCheckpoint
        << " got notified by finished blocking work, starting verify";

    if (mVerifyApplyWork)
    {
        mVerifyApplyWork->makeReady();
        advanceChildren();
    }
}

Work::State
VerifyMaybeReplayTxsSnapWork::onSuccess()
{
    if (!verifyAndMaybeReplayTxs())
    {
        return Work::WORK_PENDING;
    }

    if (mShouldApply)
    {
        mLastApplied = mApp.getLedgerManager().getLastClosedLedgerHeader();
        assert(mLastApplied.header.ledgerSeq == mCheckpoint ||
               mLastApplied.header.ledgerSeq == mRange.last());
    }

    if (getDependent())
    {
        auto applied = mShouldApply ? "and application" : "";
        CLOG(DEBUG, "History") << "Transactions for checkpoint " << mCheckpoint
                               << " completed verification " << applied
                               << ": notifying "
                                  "dependent work";
        notifyCompleted();
    }
    else
    {
        if (mCheckpoint !=
            mApp.getHistoryManager().checkpointContainingLedger(mRange.last()))
        {
            throw std::runtime_error("No dependent work found, even though the "
                                     "end of the chain was not reached.");
        }
        CLOG(DEBUG, "History")
            << "Last checkpoint: No dependent work to notify";
    }

    return Work::WORK_SUCCESS;
}

void
VerifyMaybeReplayTxsSnapWork::notify(std::string const& child)
{
    auto work = mChildren.find(child);
    if (work != mChildren.end())
    {
        assert(work->second->isDone());
        if (work->second->getState() == Work::WORK_SUCCESS)
        {
            mDownloadTxsSuccess.Mark();
        }
        else
        {
            mDownloadTxsFailure.Mark();
        }
    }
    BatchableWork::notify(child);
}
}