// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "historywork/DownloadVerifyLedgersWork.h"
#include "catchup/CatchupManager.h"
#include "history/HistoryManager.h"
#include "historywork/GetAndUnzipRemoteFileWork.h"
#include "historywork/LedgerChainVerificationSnapshotWork.h"
#include "historywork/Progress.h"
#include "lib/util/format.h"
#include "main/Application.h"

namespace stellar
{
// TODO update VS files
DownloadVerifyLedgersWork::DownloadVerifyLedgersWork(
    Application& app, WorkParent& parent, LedgerRange range,
    TmpDir const& downloadDir, LedgerHeaderHistoryEntry const& lcl,
    LedgerHeaderHistoryEntry& firstVerified, optional<Hash> scpHash)
    : BatchWork{app, parent,
                fmt::format("download-verify-ledgers-{:08x}-{:08x}",
                            range.first(), range.last())}
    , mRange{range}
    , mCheckpointRange{CheckpointRange{range, mApp.getHistoryManager()}}
    , mNext{mCheckpointRange.last()}
    , mDownloadDir{downloadDir}
    , mFirstVerified{firstVerified}
    , mLastClosedLedger{lcl}
    , mTrustedHash{scpHash}
    , mDownloadStart{app.getMetrics().NewMeter(
          {"history", "download-" + std::string(HISTORY_FILE_TYPE_LEDGER),
           "start"},
          "event")}
    , mDownloadSuccess{app.getMetrics().NewMeter(
          {"history", "download-" + std::string(HISTORY_FILE_TYPE_LEDGER),
           "success"},
          "event")}
    , mDownloadFailure{app.getMetrics().NewMeter(
          {"history", "download-" + std::string(HISTORY_FILE_TYPE_LEDGER),
           "failure"},
          "event")}
{
}

DownloadVerifyLedgersWork::~DownloadVerifyLedgersWork()
{
    clearChildren();
}

std::string
DownloadVerifyLedgersWork::getStatus() const
{
    if (mState == WORK_RUNNING || mState == WORK_PENDING)
    {
        auto task = "downloading and verifying ledger chain files";
        return fmtProgress(mApp, task, mRange.first(), mRange.last(),
                           (mRange.last() - mNext + mRange.first()));
    }
    return Work::getStatus();
}

bool
DownloadVerifyLedgersWork::hasNext()
{
    return mNext >= mCheckpointRange.first();
}

void
DownloadVerifyLedgersWork::resetIter()
{
    mNext = mCheckpointRange.last();
}

std::shared_ptr<BatchableWork>
DownloadVerifyLedgersWork::yieldMoreWork()
{
    if (!hasNext())
    {
        throw std::runtime_error("Nothing to iterate over!");
    }

    CLOG(DEBUG, "History")
        << "Downloading, unzipping, and verifying ledger header " << mNext;

    mDownloadStart.Mark();
    auto verifySnapshot = addWork<LedgerChainVerificationSnapshotWork>(
        mDownloadDir, mNext, mRange, mFirstVerified, mLastClosedLedger,
        mTrustedHash, "checkpoint-download-verify-" + std::to_string(mNext));

    mNext = mNext < mApp.getHistoryManager().getCheckpointFrequency()
                ? 0
                : mNext - mApp.getHistoryManager().getCheckpointFrequency();
    return verifySnapshot;
}

    // TODO use throw instead of assert
void
DownloadVerifyLedgersWork::notify(std::string const& child)
{
    auto work = mChildren.find(child);
    if (work != mChildren.end())
    {
        switch (work->second->getState())
        {
        case Work::WORK_SUCCESS:
            mDownloadSuccess.Mark();
            break;
        case Work::WORK_FAILURE_RETRY:
        case Work::WORK_FAILURE_FATAL:
        case Work::WORK_FAILURE_RAISE:
            mDownloadFailure.Mark();
            break;
        default:
            break;
        }
    }

    BatchWork::notify(child);
}
}
