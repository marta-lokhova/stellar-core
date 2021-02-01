// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0
#pragma once

#include "ledger/CheckpointRange.h"
#include "util/TmpDir.h"
#include "work/BatchWork.h"
#include "work/WorkSequence.h"

namespace stellar
{

class HistoryArchive;

class VerifyHASRange : public Work
{
    CheckpointRange mRange;
    std::unique_ptr<TmpDir> mDownloadDir;
    std::shared_ptr<HistoryArchive> mArchive;
    std::shared_ptr<WorkSequence> mSeq;
    uint32_t mCount{0};

  public:
    VerifyHASRange(Application& app, CheckpointRange range,
                   std::shared_ptr<HistoryArchive> archive = nullptr);
    ~VerifyHASRange() = default;

  protected:
    BasicWork::State doWork() override;
    void doReset() override;
};
}
