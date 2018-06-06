// Copyright 2018 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0
#pragma once

#include "work/Work.h"
#include <medida/meter.h>
#include <medida/metrics_registry.h>

namespace medida
{
class Meter;
}

namespace stellar
{

class TmpDir;

class BatchWork : public Work
{
    /* This class performs parallel batching of Work by throttling workers.
       Child classes must supply iteration methods, that would generate work
       they'd like to perform. This class only acts as a commander, adding more
       work if it has bandwidth.
    **/
  public:
    BatchWork(Application& app, WorkParent& parent, std::string name);
    ~BatchWork() override;
    void onReset() override;
    void notify(std::string const& child) override;

    // Optionally mark metrics
    virtual void markMetrics(Work& work){};

    virtual bool hasNext() = 0;
    virtual std::string yieldMoreWork() = 0;
    virtual void resetIter() = 0;
};
}
