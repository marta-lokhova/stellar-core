// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "historywork/LedgerChainVerificationSnapshotWork.h"
#include "history/FileTransferInfo.h"
#include "historywork/Progress.h"
#include "ledger/LedgerManager.h"

namespace stellar
{
LedgerChainVerificationSnapshotWork::LedgerChainVerificationSnapshotWork(
    Application& app, WorkParent& parent, TmpDir const& downloadDir,
    uint32_t checkpoint, LedgerRange const& range,
    LedgerHeaderHistoryEntry& firstVerified,
    LedgerHeaderHistoryEntry const& lastClosedLedger, optional<Hash> scpHash,
    std::string name)
    : BatchableWork(app, parent, name)
    , mCheckpoint(checkpoint)
    , mRange(range)
    , mFirstVerified(firstVerified)
    , mLastClosedLedger(lastClosedLedger)
    , mDownloadDir(downloadDir)
    , mTrustedHash(scpHash)
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
        mSnapshotData = BatchableWorkResultData{mVerifiedAhead};
        if (getDependent())
        {
            CLOG(DEBUG, "History")
                << "Checkpoint " << mCheckpoint
                << " completed verification: Notifying dependent work";
            notifyCompleted();
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
            CLOG(DEBUG, "History")
                << "End of ledger chain: No dependent work to notify";
        }
    }

    return Work::WORK_SUCCESS;
}

bool
LedgerChainVerificationSnapshotWork::isReady()
{
    // When last verified is set that means checkpoint ahead of us completed
    // verification. The only time this does not apply is when we begin
    // chain verification, and first ledger is instantly ready (it has
    // nothing to wait for)
    auto end = mApp.getHistoryManager().checkpointContainingLedger(
                   mRange.last()) == mCheckpoint;
    return mVerifiedAhead.header.ledgerSeq != 0 || end;
}

void
LedgerChainVerificationSnapshotWork::unblockWork(
    BatchableWorkResultData const& data)
{
    auto verified = data.mVerifiedAheadLedger;

    CLOG(DEBUG, "History")
        << "Snapshot for checkpoint " << mCheckpoint
        << " got notified by finished blocking work, starting verify";

    if (mVerifyWork || isReady())
    {
        throw std::runtime_error(
            fmt::format("Verify work for {:s} checkpoint has already been set",
                        mCheckpoint));
    }
    if (verified.header.ledgerSeq == 0)
    {
        throw std::runtime_error(
            fmt::format("Blocking work for checkpoint {:s} passed invalid data",
                        mCheckpoint));
    }

    mVerifiedAhead = verified;

    if (mDownloadWork)
    {
        switch (mDownloadWork->getState())
        {
        // Download must complete before triggering verify.
        case Work::WORK_SUCCESS:
            verifyChain();
            break;
        case Work::WORK_PENDING:
        case Work::WORK_RUNNING:
            CLOG(DEBUG, "History") << "Download work for checkpoint "
                                   << mCheckpoint << " is still in progress";
            break;
        default:
            CLOG(WARNING, "History")
                << "Download work for checkpoint " << mCheckpoint
                << " failed, notification of blocking work completion will "
                   "have no effect.";
            break;
        }
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
    if (!isReady())
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
        else
        {
            throw std::runtime_error(
                fmt::format("Verify work for checkpoint {:s} already failed.",
                            mCheckpoint));
        }
    }

    // We want to provide the following guarantee: Verify work is only scheduled
    // AFTER all blocking work completes. This includes:
    // * Dependent checkpoint verification
    // * Ledger header download
    CLOG(DEBUG, "History") << "Verifying checkpoint " << mCheckpoint;
    mVerifyWork = addWork<VerifyLedgerChainWork>(
        mDownloadDir, mCheckpoint, mRange, mFirstVerified, mVerifiedAhead,
        mLastClosedLedger, mTrustedHash);

    return false;
}
}
