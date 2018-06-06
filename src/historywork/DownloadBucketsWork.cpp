// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "DownloadBucketsWork.h"
#include "VerifyBucketWork.h"
#include "historywork/GetAndUnzipRemoteFileWork.h"
#include "lib/util/format.h"
#include "main/Application.h"

namespace stellar
{

DownloadBucketsWork::DownloadBucketsWork(
    Application& app, WorkParent& parent,
    std::map<std::string, std::shared_ptr<Bucket>>& buckets,
    std::vector<std::string> hashes, TmpDir const& downloadDir)
    : BatchWork{app, parent, "download-verify-buckets"}
    , mBuckets{buckets}
    , mHashes{hashes}
    , mDownloadDir{downloadDir}
    , mDownloadBucketStart{app.getMetrics().NewMeter(
          {"history", fmt::format("download-{:s}", HISTORY_FILE_TYPE_BUCKET),
           "start"},
          "event")}
    , mDownloadBucketSuccess{app.getMetrics().NewMeter(
          {"history", fmt::format("download-{:s}", HISTORY_FILE_TYPE_BUCKET),
           "success"},
          "event")}
    , mDownloadBucketFailure{app.getMetrics().NewMeter(
          {"history", fmt::format("download-{:s}", HISTORY_FILE_TYPE_BUCKET),
           "failure"},
          "event")}
{
}

DownloadBucketsWork::~DownloadBucketsWork()
{
    clearChildren();
}

std::string
DownloadBucketsWork::getStatus() const
{
    if (mState == WORK_RUNNING || mState == WORK_PENDING)
    {
        if (!mHashes.empty())
        {
            auto pct = 100 * mNextBucketIndex / mHashes.size();
            return fmt::format(
                "downloading and verifying buckets: {:d}/{:d} ({:d}%)",
                mNextBucketIndex, mHashes.size(), pct);
        }
    }
    return Work::getStatus();
}

bool
DownloadBucketsWork::hasNext()
{
    return mHashes.size() > mNextBucketIndex;
}

void
DownloadBucketsWork::resetIter()
{
    mNextBucketIndex = 0;
}

std::string
DownloadBucketsWork::yieldMoreWork()
{
    if (!hasNext())
    {
        throw std::runtime_error("Nothing to iterate over!");
    }

    auto hash = mHashes[mNextBucketIndex];
    FileTransferInfo ft(mDownloadDir, HISTORY_FILE_TYPE_BUCKET, hash);
    auto verify = addWork<VerifyBucketWork>(mBuckets, ft.localPath_nogz(),
                                            hexToBin256(hash));
    auto download = verify->addWork<GetAndUnzipRemoteFileWork>(ft);

    assert(mVerifyDownloadMap.find(verify->getUniqueName()) ==
           mVerifyDownloadMap.end());
    mVerifyDownloadMap.insert(
        std::make_pair(verify->getUniqueName(), download));
    mDownloadBucketStart.Mark();

    ++mNextBucketIndex;
    return verify->getUniqueName();
}

void
DownloadBucketsWork::markMetrics(Work& work)
{
    auto downloadWork = mVerifyDownloadMap.find(work.getUniqueName());
    assert(downloadWork != mVerifyDownloadMap.end());

    switch (downloadWork->second->getState())
    {
    case Work::WORK_SUCCESS:
        mDownloadBucketSuccess.Mark();
        break;
    case Work::WORK_FAILURE_RETRY:
    case Work::WORK_FAILURE_FATAL:
    case Work::WORK_FAILURE_RAISE:
        mDownloadBucketFailure.Mark();
        break;
    default:
        break;
    }

    mVerifyDownloadMap.erase(downloadWork);
}
}
