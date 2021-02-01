// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/VerifyHASRange.h"
#include "bucket/BucketManager.h"
#include "history/HistoryManager.h"
#include "historywork/DownloadBucketsWork.h"
#include "historywork/GetHistoryArchiveStateWork.h"
#include "ledger/LedgerManager.h"
#include "util/Logging.h"
#include "work/WorkSequence.h"

namespace stellar
{
class ContainsValidBucketsWork : public Work
{
    std::shared_ptr<GetHistoryArchiveStateWork> mPRevWork;
    std::shared_ptr<DownloadBucketsWork> mGetBucketsWork;
    std::map<std::string, std::shared_ptr<Bucket>> mBuckets;
    TmpDir const& mDownloadDir;

  public:
    ContainsValidBucketsWork(Application& app,
                             std::shared_ptr<GetHistoryArchiveStateWork> work,
                             TmpDir const& dir)
        : Work(app, "contains-valid-buckets")
        , mPRevWork{work}
        , mDownloadDir{dir}
    {
    }

    BasicWork::State
    doWork() override
    {
        assert(mPRevWork);
        assert(mPRevWork->getState() == BasicWork::State::WORK_SUCCESS);
        auto const& has = mPRevWork->getHistoryArchiveState();

        if (!mGetBucketsWork)
        {
            auto buckets = has.differingBuckets(
                mApp.getLedgerManager().getLastClosedLedgerHAS());
            mGetBucketsWork =
                addWork<DownloadBucketsWork>(mBuckets, buckets, mDownloadDir);
        }
        else
        {
            if (mGetBucketsWork->getState() == BasicWork::State::WORK_SUCCESS)
            {
                if (!has.containsValidBuckets(mApp))
                {
                    CLOG_ERROR(Bucket, "INVALID buckets for ledger {}",
                               has.currentLedger);
                }
                else
                {
                    CLOG_INFO(Bucket, "OK: VALID buckets for ledger {}",
                              has.currentLedger);
                }
            }
        }

        return mGetBucketsWork->getState();
    }

    void
    doReset() override
    {
        mGetBucketsWork.reset();
        mBuckets.clear();
    }

    void
    onSuccess() override
    {
        mBuckets.clear();
    }
};

VerifyHASRange::VerifyHASRange(Application& app, CheckpointRange range,
                               std::shared_ptr<HistoryArchive> archive)
    : Work{app, "download-verify-buckets"}
    , mRange{range}
    , mDownloadDir{std::make_unique<TmpDir>(
          mApp.getTmpDirManager().tmpDir("verify-has"))}
    , mArchive{archive}
{
}

void
VerifyHASRange::doReset()
{
    mSeq.reset();
    mCount = 0;
}

BasicWork::State
VerifyHASRange::doWork()
{
    ZoneScoped;
    if (!mSeq)
    {
        std::vector<std::shared_ptr<BasicWork>> mainSeq;
        for (int i = mRange.mFirst; i < mRange.limit();
             i += mApp.getHistoryManager().getCheckpointFrequency())
        {
            auto w1 =
                std::make_shared<GetHistoryArchiveStateWork>(mApp, i, mArchive);
            auto w2 = std::make_shared<ContainsValidBucketsWork>(mApp, w1,
                                                                 *mDownloadDir);
            std::vector<std::shared_ptr<BasicWork>> subSeq{w1, w2};
            auto w3 = std::make_shared<WorkSequence>(
                mApp, "download-verify-sequence-" + std::to_string(i), subSeq);
            mainSeq.push_back(w3);
        }

        mSeq = addWork<WorkSequence>("main-verify-seq", mainSeq);
    }
    if (++mCount == 500000)
    {
        CLOG_INFO(Bucket, "FORGET unreferenced buckets");
        mCount = 0;
        // Once in the while, cleanup buckets to avoid storage bloat
        mApp.getBucketManager().forgetUnreferencedBuckets();
    }
    return mSeq->getState();
}
}
