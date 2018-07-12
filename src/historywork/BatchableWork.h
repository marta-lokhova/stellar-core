// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0
#pragma once

#include "lib/util/format.h"
#include "work/Work.h"
#include "xdr/Stellar-ledger.h"

namespace stellar
{

class BatchableWork : public Work
{
    // Class that provides skeleton for snapshot classes.
    // Child classes can create dependencies by overriding
    // registerDependent and notifyCompleted. This is useful
    // if we need to enforce a DAG in the work structure.
    // For example, when verifying ledger chain, current
    // work needs to wait for the previous work to complete
    // in order to proceed.

    std::shared_ptr<BatchableWork> mDependentSnapshot;

  protected:
    struct BatchableWorkResultData
    {
        LedgerHeaderHistoryEntry mVerifiedAheadLedger;
        // ...add more useful stuff...
    };

    BatchableWorkResultData mSnapshotData{};

  public:
    BatchableWork(Application& app, WorkParent& parent, std::string name,
                  size_t maxRetries = RETRY_A_FEW)
        : Work(app, parent, fmt::format("batchable-" + name), maxRetries)
    {
    }

    virtual ~BatchableWork()
    {
    }

    virtual void
    registerDependent(std::shared_ptr<BatchableWork> blockedWork)
    {
        mDependentSnapshot = blockedWork;
    };

    virtual void
    notifyCompleted()
    {
        if (mDependentSnapshot)
        {
            mDependentSnapshot->unblockWork(mSnapshotData);
        }
    };

    // Each subclass decides how to proceed
    virtual void unblockWork(BatchableWorkResultData const& data){};

    std::shared_ptr<BatchableWork>
    getDependent()
    {
        return mDependentSnapshot;
    }
};
}
