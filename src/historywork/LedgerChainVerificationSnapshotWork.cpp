// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "historywork/LedgerChainVerificationSnapshotWork.h"
#include "history/FileTransferInfo.h"
#include "historywork/Progress.h"
#include "ledger/LedgerManager.h"
#include "main/Application.h"

namespace stellar
{
LedgerChainVerificationSnapshotWork::LedgerChainVerificationSnapshotWork(
    Application& app, WorkParent& parent, TmpDir const& downloadDir,
    uint32_t checkpoint, LedgerRange range,
    LedgerHeaderHistoryEntry& firstVerified,
    LedgerHeaderHistoryEntry const& lastClosedLedger, optional<Hash> scpHash,
    std::string name)
    : BatchableWork(app, parent, name)
    , mCheckpoint{checkpoint}
    , mRange{range}
    , mFirstVerified{firstVerified}
    , mLastClosedLedger{lastClosedLedger}
    , mDownloadDir{downloadDir}
    , mTrustedHash{scpHash}

{
}

LedgerChainVerificationSnapshotWork::~LedgerChainVerificationSnapshotWork()
{
    clearChildren();
}

std::string
LedgerChainVerificationSnapshotWork::getStatus() const
{
    if (mState == WORK_PENDING)
    {
        if (mVerifyWork)
        {
            return mVerifyWork->getStatus();
        }
        else if (mDownloadWork)
        {
            return mDownloadWork->getStatus();
        }
    }

    return BatchableWork::getStatus();
}

void
LedgerChainVerificationSnapshotWork::onReset()
{
    mDownloadWork.reset();
    mVerifyWork.reset();
}

Work::State
LedgerChainVerificationSnapshotWork::onSuccess()
{
    if (!downloadChain())
    {
        return Work::WORK_PENDING;
    }
    else if (!verifyChain())
    {
        return Work::WORK_PENDING;
    }
    else
    {
        mSnapshotData =  BatchableWorkResultData{mVerifiedAhead};
        if (mDependentSnapshot)
        {
            CLOG(INFO, "History")
                << "Checkpoint " << mCheckpoint
                << " completed verification: Notifying dependent work";
            notifyCompleted(mSnapshotData);
        }
        else
        {
            if (mCheckpoint !=
                mApp.getHistoryManager().checkpointContainingLedger(
                    mRange.first()))
            {
                throw std::runtime_error(
                    "No dependent work found, even though the "
                    "end of the chain was not reached.");
            }
            CLOG(INFO, "History")
                << "End of ledger chain: No dependent work to notify";
        }
    }

    return Work::WORK_SUCCESS;
}

void
LedgerChainVerificationSnapshotWork::unblockWork(
        BatchableWorkResultData const& data)
{
    auto verified = data.mVerifiedAheadLedger;

    CLOG(INFO, "History")
        << "Snapshot for checkpoint " << mCheckpoint
        << " got notified by finished blocking work. Starting verify...";

    // Ensure work hasn't been triggered already
    assert(!mVerifyWork);
    assert(!isReady());
    assert(verified.header.ledgerSeq != 0);

    mVerifiedAhead = verified;

    // Download MUST complete before triggering verify
    if (mDownloadWork && mDownloadWork->getState() == Work::WORK_SUCCESS)
    {
        verifyChain();
    }
}

bool
LedgerChainVerificationSnapshotWork::downloadChain()
{
    if (mDownloadWork)
    {
        assert(mDownloadWork->getState() == Work::WORK_SUCCESS);
        return true;
    }

    CLOG(DEBUG, "History") << "Downloading ledgers for checkpoint "
                           << mCheckpoint;
    FileTransferInfo ft{mDownloadDir, HISTORY_FILE_TYPE_LEDGER, mCheckpoint};
    mDownloadWork = addWork<GetAndUnzipRemoteFileWork>(ft);
    return false;
}

bool
LedgerChainVerificationSnapshotWork::verifyChain()
{
    auto end = mApp.getHistoryManager().checkpointContainingLedger(
                   mRange.last()) == mCheckpoint;
    if (!isReady() && !end)
    {
        // Verify is still waiting on dependent work
        return false;
    }

    if (mVerifyWork)
    {
        auto state = mVerifyWork->getState();
        if (state == Work::WORK_SUCCESS)
        {
            return true;
        }
        else if (state == Work::WORK_RUNNING || state == Work::WORK_PENDING)
        {
            return false;
        }
    }

    // We want to provide the following guarantee: Verify work is only scheduled
    // AFTER all work we depend on completes. This includes:
    // * Previous checkpoint verification
    // * Ledger header download
    CLOG(DEBUG, "History") << "Verifying checkpoint " << mCheckpoint;
    mVerifyWork = addWork<VerifyLedgerChainWork>(
        mDownloadDir, mCheckpoint, mRange, mFirstVerified, mVerifiedAhead,
        mLastClosedLedger, mTrustedHash);

    return false;
}
}
