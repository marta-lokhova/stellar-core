#include "overlay/OverlayProcessingWork.h"
#include "crypto/BLAKE2.h"
#include "crypto/Hex.h"
#include "crypto/Random.h"
#include "crypto/SHA.h"
#include "herder/Herder.h"
#include "medida/metrics_registry.h"
#include "medida/timer.h"
#include "overlay/OverlayManager.h"
#include "overlay/OverlayMetrics.h"
#include "util/Logging.h"
#include "work/WorkWithCallback.h"
#include <fmt/format.h>

namespace stellar
{

// This makes it much easier to track a unit of work
// TODO: what if we move broadcasting to this work? Out of pending envelopes
// TODO: add some metrics
class ProcessSCPMessageWork : public BasicWork
{
    bool mStarted{false};
    Peer::BroadcastToPeers mPeersToBroadcast;
    Peer::DoneCallback mDoneCallback;
    StellarMessage mMsg;
    std::weak_ptr<Peer> mPeer;
    std::unique_ptr<Herder::EnvelopeStatus> mEnvelopeStatus;

  public:
    Peer::BroadcastToPeers
    getPeersToBroadcast() const
    {
        assert(getState() == State::WORK_SUCCESS);
        return mPeersToBroadcast;
    }

    ProcessSCPMessageWork(Application& app, StellarMessage const& stellarMsg,
                          std::weak_ptr<Peer> peer, Peer::DoneCallback cb)
        // TODO: better work names
        : BasicWork(app,
                    fmt::format("overlay-message-processing-{}",
                                binToHex(randomBytes(8))),
                    BasicWork::RETRY_NEVER)
        , mMsg(stellarMsg)
        , mPeer(peer)
        , mDoneCallback(cb)
    {
        assert(mMsg.type() == SCP_MESSAGE);
    }

    // TODO: return onStart to Work
    // TODO: brin back the timer
    // auto t = getOverlayMetrics().mRecvSCPMessageTimer.TimeScope();
    bool
    onAbort() override
    {
        return true;
    }

    void
    onReset() override
    {
        mStarted = false;
        mEnvelopeStatus.reset();
    }

    void
    onSuccess() override
    {
        // Centralize envelope broadcasting, make it responsibility of SCP
        // message processing work (not pending envelopes)
        assert(mEnvelopeStatus);
        if (*mEnvelopeStatus == Herder::ENVELOPE_STATUS_READY)
        {
            mPeersToBroadcast =
                mApp.getOverlayManager().broadcastMessage(mMsg, mDoneCallback);
        }
    }

    State
    onRun() override
    {
        // TODO: add Tracy ZoneText
        auto peer = mPeer.lock();
        if (!peer)
        {
            CLOG_ERROR(Work, "PEER does not exist!");
            return State::WORK_FAILURE;
        }

        if (!mStarted)
        {
            mStarted = true;

            // add it to the floodmap so that this peer gets credit for it
            Hash msgID;
            mApp.getOverlayManager().recvFloodedMsgID(mMsg, peer, msgID);

            mEnvelopeStatus = std::make_unique<Herder::EnvelopeStatus>(
                mApp.getHerder().recvSCPEnvelope(mMsg.envelope()));
            if (*mEnvelopeStatus == Herder::ENVELOPE_STATUS_DISCARDED)
            {
                // the message was discarded, remove it from the floodmap as
                // well
                mApp.getOverlayManager().forgetFloodedMsg(msgID);
            }

            // If not fetching, it means we have finished processing envelope
            if (*mEnvelopeStatus != Herder::ENVELOPE_STATUS_FETCHING)
            {
                return State::WORK_SUCCESS;
            }
        }

        // Mean we're fetching if got to here, setup a timer. Order of magnitude
        // is in milliseconds here to account for network latency
        if (!mApp.getHerder().isFullyFetched(mMsg.envelope()))
        {
            setupWaitingCallback(std::chrono::milliseconds(1));
            return State::WORK_WAITING;
        }
        else
        {
            mEnvelopeStatus = std::make_unique<Herder::EnvelopeStatus>(
                mApp.getHerder().recvSCPEnvelope(mMsg.envelope()));
            // Since we're done fetching, ensure the envelope state is
            // "finished"
            assert(*mEnvelopeStatus != Herder::ENVELOPE_STATUS_FETCHING);
        }
        return State::WORK_SUCCESS;
    }
};

ProcessMessageAndWriteToNetwork::ProcessMessageAndWriteToNetwork(
    Application& app, StellarMessage const& msg, std::weak_ptr<Peer> peer)
    : Work(
          app,
          fmt::format("process-message-and-write-{}", binToHex(randomBytes(8))),
          BasicWork::RETRY_NEVER)
    , mMsg(msg)
    , mPeer(peer)
    , mPeerOverallTimer(app.getMetrics().NewTimer(
          {"overlay", (peer.lock() ? peer.lock()->toString() : "peer"),
           "timing-per-message"}))
    , mStartTime(mApp.getClock().now())
{
}

void
ProcessMessageAndWriteToNetwork::doReset()
{
    mSCPMessageProcessingWork.reset();
    mStartTime = mApp.getClock().now();
    mStarted = false;
    mNumWrites = 0;
    mExpectedWrites = 0;
}

void
ProcessMessageAndWriteToNetwork::onSuccess()
{
    // These metrics are mo accurate now, as they track how long it takes to
    // process a message completely
    auto time = mApp.getClock().now() - mStartTime;
    auto& om = mApp.getOverlayManager().getOverlayMetrics();

    // Update timing for all messages for this peer
    // Will will get a rate for this peer as well
    mPeerOverallTimer.Update(time);

    switch (mMsg.type())
    {
    case SCP_MESSAGE:
    {
        om.mRecvSCPMessageTimer.Update(time);
        auto type = mMsg.envelope().statement.pledges.type();
        if (type == SCP_ST_PREPARE)
        {
            om.mRecvSCPPrepareTimer.Update(time);
        }
        else if (type == SCP_ST_CONFIRM)
        {
            om.mRecvSCPConfirmTimer.Update(time);
        }
        else if (type == SCP_ST_EXTERNALIZE)
        {
            om.mRecvSCPExternalizeTimer.Update(time);
        }
        else
        {
            om.mRecvSCPNominateTimer.Update(time);
        }
        break;
    }

    case TRANSACTION:
    {
        om.mRecvTransactionTimer.Update(time);
        break;
    }

    case GET_TX_SET:
    {
        om.mRecvGetTxSetTimer.Update(time);
        break;
    }
    case GET_SCP_QUORUMSET:
    {
        om.mRecvGetSCPQuorumSetTimer.Update(time);
        break;
    }
    case GET_SCP_STATE:
    {
        om.mRecvGetSCPStateTimer.Update(time);
        break;
    }
    case TX_SET:
    {
        om.mRecvTxSetTimer.Update(time);
        break;
    }
    case SCP_QUORUMSET:
    {
        om.mRecvSCPQuorumSetTimer.Update(time);
        break;
    }
    case PEERS:
    {
        om.mRecvPeersTimer.Update(time);
        break;
    }
    case SURVEY_REQUEST:
    {
        om.mRecvSurveyRequestTimer.Update(time);
        break;
    }
    case SURVEY_RESPONSE:
    {
        om.mRecvSurveyResponseTimer.Update(time);
        break;
    }
    case GET_PEERS:
    {
        om.mRecvGetPeersTimer.Update(time);
        break;
    }
    case DONT_HAVE:
    {
        om.mRecvDontHaveTimer.Update(time);
        break;
    }
    default:
        assert(false);
    }
}

// For broadcaring, assumptions: peers can be dropped, so we can't just count
// based on a set number Work could keep references to peers?
BasicWork::State
ProcessMessageAndWriteToNetwork::doWork()
{
    // TODO: all new works are missing onReset

    // 1. First iteration, spin up whatever work
    if (!mStarted)
    {
        auto peer = mPeer.lock();
        if (!peer)
        {
            CLOG_ERROR(Work, "Peer does not exist!");
            return State::WORK_FAILURE;
        }

        std::weak_ptr<ProcessMessageAndWriteToNetwork> weak(
            static_pointer_cast<ProcessMessageAndWriteToNetwork>(
                shared_from_this()));
        auto done = [weak, msg = mMsg]() {
            auto self = weak.lock();
            if (self && !self->isDone())
            {
                // Another write completed
                self->mNumWrites++;
                self->wakeUp();
            }
        };

        switch (mMsg.type())
        {
        case SCP_MESSAGE:
        {
            mSCPMessageProcessingWork =
                addWork<ProcessSCPMessageWork>(mMsg, mPeer, done);
            break;
        }

        case TRANSACTION:
        {
            // Do not keep track on broadcasting, let the transaction queue
            // handle throttling
            peer->recvTransaction(mMsg);
            break;
        }

        case GET_TX_SET:
        {
            // Write back to the same peer
            mPeersToWriteTo = peer->recvGetTxSet(mMsg, done);
            break;
        }
        case GET_SCP_QUORUMSET:
        {
            // Write back to the same peer
            mPeersToWriteTo = peer->recvGetSCPQuorumSet(mMsg, done);
            break;
        }
        case GET_SCP_STATE:
        {
            mPeersToWriteTo = peer->recvGetSCPState(mMsg, done);
            break;
        }
        case TX_SET:
        {
            // Doesn't write to the network
            peer->recvTxSet(mMsg);
            break;
        }
        case SCP_QUORUMSET:
        {
            // Doesn't write to the network
            peer->recvSCPQuorumSet(mMsg);
            break;
        }
        case PEERS:
        {
            // Doesn't write to the network
            peer->recvGetPeers(mMsg);
            break;
        }
        case SURVEY_REQUEST:
        {
            // TODO: deal with this later, not used
            peer->recvSurveyRequestMessage(mMsg);
            break;
        }
        case SURVEY_RESPONSE:
        {
            // TODO: deal with this later, not used
            peer->recvSurveyResponseMessage(mMsg);
            break;
        }
        case GET_PEERS:
        {
            // TODO: deal with this later, not used
            peer->recvGetPeers(mMsg);
            break;
        }
        case DONT_HAVE:
        {
            // TODO: deal with this later, we receive DONT_HAVE rarely
            peer->recvDontHave(mMsg);
            break;
        }
        default:
            assert(false);
        }

        mStarted = true;
        return State::WORK_RUNNING;
    }
    else
    {
        // Check if we are still waiting for SCP message to finish
        if (mSCPMessageProcessingWork)
        {
            assert(mMsg.type() == SCP_MESSAGE);
            if (mSCPMessageProcessingWork->getState() != State::WORK_SUCCESS)
            {
                return mSCPMessageProcessingWork->getState();
            }
            else
            {
                mPeersToWriteTo =
                    mSCPMessageProcessingWork->getPeersToBroadcast();
            }
        }
    }

    // Purge any dropped connections
    // TODO: verify that this works as expected; outline why we need it
    auto num = mPeersToWriteTo.size();
    for (auto it = mPeersToWriteTo.begin(); it != mPeersToWriteTo.end();)
    {
        auto strong = it->lock();
        if (!strong)
        {
            it = mPeersToWriteTo.erase(it);
        }
        else
        {
            it++;
        }
    }

    // Figure out how many writes are we waiting for
    mExpectedWrites = mPeersToWriteTo.empty()
                          ? 0
                          : std::max<size_t>(1, mPeersToWriteTo.size() / 2);

    if (mMsg.type() == SCP_MESSAGE && num != 0)
    {
        CLOG_DEBUG(Work,
                   "BROADCAST: original {}, after trimming {}, expected {}",
                   num, mPeersToWriteTo.size(), mExpectedWrites);
    }
    return mNumWrites >= mExpectedWrites ? State::WORK_SUCCESS
                                         : State::WORK_WAITING;
}

OverlayProcessingWork::OverlayProcessingWork(Application& app,
                                             std::weak_ptr<Peer> peer)
    : Work(app, "overlay-processing-work", BasicWork::RETRY_NEVER)
    , mPeer(peer)
    , mMessagesInFlightMeter(app.getMetrics().NewCounter(
          {"overlay", "async-processing", "in-flight"}))
    , mMessagesProcessedMeter(app.getMetrics().NewMeter(
          {"overlay", "async-processing", "processed"}, "message"))
{
    assert(mPeer.lock());
}

int
OverlayProcessingWork::countFinished()
{
    int processed = 0;
    // Clean up completed children
    for (auto childIt = mCurrentWorks.begin(); childIt != mCurrentWorks.end();)
    {
        if ((*childIt)->getState() == State::WORK_SUCCESS)
        {
            // CLOG_DEBUG(Work, "Finished child work {}", childIt->first);
            childIt = mCurrentWorks.erase(childIt);
            processed++;
        }
        else
        {
            ++childIt;
        }
    }

    return processed;
}

// TODO: for some reason, authenticated peers is not shown int he metrics
BasicWork::State
OverlayProcessingWork::doWork()
{
    // Grab the valid peer
    auto peer = mPeer.lock();
    if (!peer)
    {
        CLOG_ERROR(Work, "Peer does not exist!");
        // Peer has been dropped, do not process the message (current behavior
        // in master @ Peer.cpp:624)
        return State::WORK_FAILURE;
    }

    auto processed = countFinished();
    mMessagesProcessedMeter.Mark(processed);

    mMessagesInFlightMeter.set_count(mCurrentWorks.size());

    CLOG_DEBUG(Work, "OverlayProcessingWork complete {} items", processed);

    // Grab whatever messages we read and create appropriate works
    auto msgs = peer->popNewMessages();
    for (auto const& msg : msgs)
    {
        auto w = addWork<ProcessMessageAndWriteToNetwork>(msg, peer);
        mCurrentWorks.emplace_back(w);
    }

    // If no work to do, wait until callback wakes us up. Yield until then
    if (anyChildRunning())
    {
        return State::WORK_RUNNING;
    }
    return State::WORK_WAITING;
}
}