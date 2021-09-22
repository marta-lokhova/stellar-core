#pragma once

#include "overlay/Peer.h"
#include "work/Work.h"

namespace medida
{
class Timer;
class Meter;
}

namespace stellar
{
class ProcessSCPMessageWork;

// Callback return whether it is done executing AND who to broadcast to!
using CallbackReturnPair = std::pair<bool, Peer::BroadcastToPeers>;
class OverlayProcessingWork : public Work
{
    std::weak_ptr<Peer> mPeer;
    std::vector<std::shared_ptr<BasicWork>> mCurrentWorks;
    int countFinished();

    medida::Counter& mMessagesInFlightMeter;
    medida::Meter& mMessagesProcessedMeter;

  public:
    OverlayProcessingWork(Application& app, std::weak_ptr<Peer> peer);
    ~OverlayProcessingWork() = default;
    State doWork() override;
};

class ProcessMessageAndWriteToNetwork : public Work
{
    bool mStarted{false};
    int mExpectedWrites{0};
    int mNumWrites{0};

    medida::Timer& mPeerOverallTimer;
    VirtualClock::time_point mStartTime;
    Peer::BroadcastToPeers mPeersToWriteTo;
    std::weak_ptr<Peer> mPeer;
    StellarMessage const mMsg;
    std::shared_ptr<ProcessSCPMessageWork> mSCPMessageProcessingWork;

  public:
    ProcessMessageAndWriteToNetwork(Application& app, StellarMessage const& msg,
                                    std::weak_ptr<Peer> peer);
    State doWork() override;

    void onSuccess() override;

    void doReset() override;
};

}