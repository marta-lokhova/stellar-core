// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "history/FileTransferInfo.h"
#include "historywork/BatchWork.h"
#include "ledger/CheckpointRange.h"
#include "ledger/LedgerRange.h"

namespace stellar
{
class VerifyMaybeReplayTxsWork : public BatchWork
{
    // TODO add comment with info

    bool mShouldApply;
    LedgerRange mRange;
    CheckpointRange mCheckpointRange;
    uint32_t mNext;
    TmpDir const& mDownloadDir;
    LedgerHeaderHistoryEntry& mLastApplied;

  public:
    VerifyMaybeReplayTxsWork(Application& app, WorkParent& parent,
                             LedgerRange range, TmpDir const& downloadDir,
                             bool apply, LedgerHeaderHistoryEntry& lastApplied);
    ~VerifyMaybeReplayTxsWork() override;
    std::string getStatus() const override;
    void onReset() override;

    bool hasNext() override;
    std::shared_ptr<BatchableWork> yieldMoreWork() override;
    void resetIter() override;
};
}
