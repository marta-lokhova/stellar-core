#pragma once

// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "fmt/format.h"
#include "main/Application.h"
#include "util/Thread.h"
#include <Tracy.hpp>

namespace stellar
{

template <typename K, typename T> class BackgroundTask
{
    Application& mApp;

    std::map<K, std::future<std::unordered_map<K, T>>> mFutures;
    std::string mName;

    uint32_t const mBatchSize;
    std::unordered_map<K, std::function<T()>> mFns;

  public:
    // Pass function you want to be executed in the background; in return you
    // get a get a future to check on
    BackgroundTask(Application& app, std::string taskName, uint32_t batchSize)
        : mApp(app), mName(std::move(taskName)), mBatchSize(batchSize)
    {
        mFns.reserve(mBatchSize);
    }
    bool
    addTask(std::function<T()>&& f, K id)
    {
        ZoneScoped;
        mFns.emplace(id, std::move(f));

        if (mFns.size() >= mBatchSize)
        {
            using task_t = std::packaged_task<std::unordered_map<K, T>()>;
            auto fn = [fns = std::move(mFns)]()
            {
                std::unordered_map<K, T> res;
                res.reserve(fns.size());
                for (auto const& f : fns)
                {
                    res.emplace(f.first, f.second());
                }
                return res;
            };

            {
                ZoneNamedN(xdrZone, "post future", true);
                auto task = std::make_shared<task_t>(fn);
                mFutures.emplace(id, task->get_future());
                mApp.postOnBackgroundThread(
                    bind(&task_t::operator(), task),
                    fmt::format("background: {}", mName),
                    Application::TaskPriority::HIGH);
            }
            mFns.clear();
            return true;
        }

        return false;
    }

    std::unordered_map<K, T>
    getResult()
    {
        ZoneScoped;

        auto it = mFutures.begin();
        if (it != mFutures.end())
        {
            ZoneNamedN(xdrZone, "resolve futures", true);
            assert(it->second.valid());
            auto res = it->second.get();
            mFutures.erase(it);
            return res;
        }

        // Asking for result before we could kick off background tasks, just
        // execute synchronously
        std::unordered_map<K, T> res;
        res.reserve(mFns.size());
        std::transform(mFns.begin(), mFns.end(), std::inserter(res, res.end()),
                       [](std::pair<K, std::function<T()>> x) {
                           return std::pair<K, T>(x.first, x.second());
                       });
        mFns.clear();
        return res;
    }
};
}
