// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0
#pragma once

#include "BatchableWork.h"
#include "catchup/ApplyLedgerChainWork.h"
#include "herder/TxSetFrame.h"
#include "history/HistoryManager.h"
#include "historywork/BatchWork.h"
#include "ledger/LedgerRange.h"
#include "main/Application.h"
#include "util/TmpDir.h"
#include <medida/meter.h>
#include <medida/metrics_registry.h>

namespace stellar
{
class VerifyMaybeReplayTxsSnapWork : public BatchableWork
{
    // Coordinator class for transaction verification
    // and application. Ensures that verification and application
    // can only proceed when upstream work (e.g. previous checkpoint)
    // has been verified and applied.

    TmpDir const& mDownloadDir;
    uint32_t mCheckpoint;
    LedgerRange const& mRange;
    LedgerHeaderHistoryEntry& mLastApplied;
    bool mShouldApply;

    std::shared_ptr<ApplyLedgerChainWork> mVerifyApplyWork;

    // Download Metrics
    medida::Meter& mDownloadTxsStart;
    medida::Meter& mDownloadTxsSuccess;
    medida::Meter& mDownloadTxsFailure;

    bool verifyAndMaybeReplayTxs();

    // TODO (mlokhava) add more asserts for abort scenarios

  protected:
    void unblockWork(BatchableWorkResultData const& data) override;

  public:
    VerifyMaybeReplayTxsSnapWork(Application& app, WorkParent& parent,
                                 TmpDir const& downloadDir, uint32_t checkpoint,
                                 LedgerRange const& range,
                                 LedgerHeaderHistoryEntry& lastApplied,
                                 bool shouldApply);
    ~VerifyMaybeReplayTxsSnapWork() override;

    std::string getStatus() const override;
    void onReset() override;
    Work::State onSuccess() override;
    void notify(std::string const& child) override;
};
}
