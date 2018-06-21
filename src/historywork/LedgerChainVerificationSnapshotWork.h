// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "catchup/VerifyLedgerChainWork.h"
#include "historywork/BatchableWork.h"
#include "historywork/GetAndUnzipRemoteFileWork.h"
#include "historywork/LedgerChainVerificationSnapshotWork.h"

namespace stellar
{

class TmpDir;
struct LedgerHeaderHistoryEntry;

class LedgerChainVerificationSnapshotWork : public BatchableWork
{
    // Class that coordinates download and verification logistics.
    // Additionally, it implements notification mechanism and
    // prevents blocked work from running.

  protected:
    void unblockWork(BatchableWorkResultData const& data) override;

  private:
    std::shared_ptr<GetAndUnzipRemoteFileWork> mDownloadWork;
    std::shared_ptr<VerifyLedgerChainWork> mVerifyWork;

    bool downloadChain();
    bool verifyChain();

    uint32_t mCheckpoint;
    LedgerRange mRange;
    LedgerHeaderHistoryEntry& mFirstVerified;
    LedgerHeaderHistoryEntry const& mLastClosedLedger;
    TmpDir const& mDownloadDir;
    optional<Hash> mTrustedHash;

    LedgerHeaderHistoryEntry mVerifiedAhead;

  public:
    LedgerChainVerificationSnapshotWork(
        Application& app, WorkParent& parent, TmpDir const& downloadDir,
        uint32_t checkpoint, LedgerRange range,
        LedgerHeaderHistoryEntry& firstVerified,
        LedgerHeaderHistoryEntry const& lastClosedLedger,
        optional<Hash> scpHash, std::string workName);
    ~LedgerChainVerificationSnapshotWork() override;
    std::string getStatus() const override;
    void onReset() override;
    Work::State onSuccess() override;

    uint32_t
    getCheckpointLedger()
    {
        return mCheckpoint;
    }

    bool
    isReady()
    {
        // When last verified is set that means checkpoint ahead of us completed
        // verification
        return mVerifiedAhead.header.ledgerSeq != 0;
    }
};
}
