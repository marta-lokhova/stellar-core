// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/ApplyLedgerChainWork.h"
#include "herder/LedgerCloseData.h"
#include "history/FileTransferInfo.h"
#include "history/HistoryManager.h"
#include "historywork/Progress.h"
#include "ledger/CheckpointRange.h"
#include "ledger/LedgerManager.h"
#include "lib/xdrpp/xdrpp/printer.h"
#include "main/Application.h"
#include "util/format.h"
#include <medida/meter.h>
#include <medida/metrics_registry.h>

namespace stellar
{
ApplyLedgerChainWork::ApplyLedgerChainWork(Application& app, WorkParent& parent,
                                           TmpDir const& downloadDir,
                                           uint32_t checkpoint,
                                           LedgerRange const& range,
                                           bool shouldApply)
    : Work(app, parent, "verify-txs-" + std::to_string(checkpoint), RETRY_NEVER)
    , mDownloadDir(downloadDir)
    , mCurrCheckpoint(checkpoint)
    , mRange(range)
    , mShouldApply(shouldApply)
    , mApplyLedgerStart(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "start"}, "event"))
    , mApplyLedgerSkip(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "skip"}, "event"))
    , mApplyLedgerSuccess(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "success"}, "event"))
    , mApplyLedgerFailureInvalidHash(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "failure-invalid-hash"}, "event"))
    , mApplyLedgerFailurePastCurrent(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "failure-past-current"}, "event"))
    , mApplyLedgerFailureInvalidLCLHash(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "failure-LCL-hash"}, "event"))
    , mApplyLedgerFailureInvalidTxSetHash(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "failure-tx-set-hash"}, "event"))
    , mApplyLedgerFailureInvalidResultHash(app.getMetrics().NewMeter(
          {"history", "apply-ledger", "failure-result-hash"}, "event"))
{
}

ApplyLedgerChainWork::~ApplyLedgerChainWork()
{
    clearChildren();
}

uint32_t
ApplyLedgerChainWork::checkpointStart()
{
    auto begin = mApp.getHistoryManager().prevCheckpointLedger(mCurrCheckpoint);
    if (begin == 0)
    {
        begin = LedgerManager::GENESIS_LEDGER_SEQ;
    }
    return begin;
}

std::string
ApplyLedgerChainWork::getStatus() const
{
    if (mState == WORK_PENDING)
    {
        return fmt::format("Verify work for checkpoint {:d} is waiting"
                           " for either download or upstream verify work",
                           mCurrCheckpoint);
    }
    if (mState == WORK_RUNNING)
    {
        auto applying = mShouldApply ? "and applying" : "";
        return fmt::format("Verifying {:s} transactions for ledger {:d}",
                           applying, mCurrLedger);
    }
    return Work::getStatus();
}

void
ApplyLedgerChainWork::onReset()
{
    auto applying = mShouldApply ? "and applying" : "";
    CLOG(INFO, "History") << "Verifying " << applying << " contents of "
                          << mCurrCheckpoint << " transaction-history files";
    mHdrIn.close();
    mTxIn.close();
    mCurrLedger = checkpointStart();
}

void
ApplyLedgerChainWork::openCurrentInputFiles()
{
    mHdrIn.close();
    mTxIn.close();
    FileTransferInfo hi(mDownloadDir, HISTORY_FILE_TYPE_LEDGER,
                        mCurrCheckpoint);
    FileTransferInfo ti(mDownloadDir, HISTORY_FILE_TYPE_TRANSACTIONS,
                        mCurrCheckpoint);
    CLOG(DEBUG, "History") << "Verifying transactions from "
                           << ti.localPath_nogz();
    mHdrIn.open(hi.localPath_nogz());
    mTxIn.open(ti.localPath_nogz());
    mTxHistoryEntry = TransactionHistoryEntry();
}

void
ApplyLedgerChainWork::onStart()
{
    openCurrentInputFiles();
}

void
ApplyLedgerChainWork::verifyMaybeApply()
{
    LedgerHeaderHistoryEntry hHeader;
    LedgerHeader& header = hHeader.header;

    if (!mHdrIn || !mHdrIn.readOne(hHeader))
    {
        throw std::runtime_error(
            fmt::format("Error reading {:d} ledger header file", mCurrLedger));
    }

    if (header.ledgerSeq != mCurrLedger)
    {
        throw std::runtime_error(
            fmt::format("Expected to parse ledger {:d} but got {:d} instead",
                        mCurrLedger, header.ledgerSeq));
    }

    auto txset = getCurrentTxSet(hHeader);
    CLOG(DEBUG, "History") << "Ledger " << header.ledgerSeq << " has "
                           << txset->size() << " transactions";

    // TODO is this secure?
    if (header.ledgerSeq > LedgerManager::GENESIS_LEDGER_SEQ)
    {
        auto computedHash = txset->getContentsHash();
        if (header.scpValue.txSetHash != computedHash)
        {
            throw std::runtime_error(
                fmt::format("Hash of transactions does not agree with txset "
                            "hash in ledger header: "
                            "txset hash for {:d} is {:s}, but expected {:s}",
                            header.ledgerSeq, hexAbbrev(computedHash),
                            hexAbbrev(header.scpValue.txSetHash)));
        }

        if (mShouldApply)
        {
            applyLedger(hHeader, txset);
        }
    }
}

void
ApplyLedgerChainWork::applyLedger(LedgerHeaderHistoryEntry lh,
                                  TxSetFramePtr txSet)
{
    assert(mShouldApply);
    assert(txSet);

    mApplyLedgerStart.Mark();

    // Ensure header was properly set from verify work
    auto& header = lh.header;
    if (header.ledgerSeq == 0)
    {
        throw std::runtime_error("Ledger header for checkpoint is not set!");
    }

    auto& lm = mApp.getLedgerManager();
    auto const& lclHeader = lm.getLastClosedLedgerHeader();

    // If we are >1 before LCL, skip
    if (header.ledgerSeq + 1 < lclHeader.header.ledgerSeq)
    {
        CLOG(DEBUG, "History")
            << "Catchup skipping old ledger " << header.ledgerSeq;
        mApplyLedgerSkip.Mark();
        CLOG(DEBUG, "History")
            << "Catchup skipping old ledger " << lclHeader.header.ledgerSeq;
        return;
    }

    // If we are one before LCL, check that we knit up with it
    if (header.ledgerSeq + 1 == lclHeader.header.ledgerSeq)
    {
        if (lh.hash != lclHeader.header.previousLedgerHash)
        {
            throw std::runtime_error(
                fmt::format("replay of {:s} failed to connect on hash of LCL "
                            "predecessor {:s}",
                            LedgerManager::ledgerAbbrev(lh),
                            LedgerManager::ledgerAbbrev(
                                lclHeader.header.ledgerSeq - 1,
                                lclHeader.header.previousLedgerHash)));
        }
        CLOG(DEBUG, "History") << "Catchup at 1-before LCL ("
                               << header.ledgerSeq << "), hash correct";
        mApplyLedgerSkip.Mark();
        return;
    }

    // If we are at LCL, check that we knit up with it
    if (header.ledgerSeq == lclHeader.header.ledgerSeq)
    {
        if (lh.hash != lm.getLastClosedLedgerHeader().hash)
        {
            mApplyLedgerFailureInvalidHash.Mark();
            throw std::runtime_error(
                fmt::format("replay of {:s} at LCL {:s} disagreed on hash",
                            LedgerManager::ledgerAbbrev(lh),
                            LedgerManager::ledgerAbbrev(lclHeader)));
        }
        CLOG(DEBUG, "History")
            << "Catchup at LCL=" << header.ledgerSeq << ", hash correct";
        mApplyLedgerSkip.Mark();
        return;
    }

    // If we are past current, we can't catch up: fail.
    if (header.ledgerSeq != lm.getCurrentLedgerHeader().ledgerSeq)
    {
        mApplyLedgerFailurePastCurrent.Mark();
        throw std::runtime_error(fmt::format(
            "replay overshot current ledger: {:d} > {:d}", header.ledgerSeq,
            lm.getCurrentLedgerHeader().ledgerSeq));
    }

    // If we do not agree about LCL hash, we can't catch up: fail.
    if (header.previousLedgerHash != lclHeader.hash)
    {
        mApplyLedgerFailureInvalidLCLHash.Mark();
        throw std::runtime_error(fmt::format(
            "replay at current ledger {:s} disagreed on LCL hash {:s}",
            LedgerManager::ledgerAbbrev(header.ledgerSeq - 1,
                                        header.previousLedgerHash),
            LedgerManager::ledgerAbbrev(lclHeader)));
    }

    LedgerCloseData closeData(header.ledgerSeq, txSet, header.scpValue);
    lm.closeLedger(closeData);

    auto const& newLclHeader = lm.getLastClosedLedgerHeader();
    CLOG(DEBUG, "History") << "LedgerManager LCL:\n"
                           << xdr::xdr_to_string(newLclHeader);
    CLOG(DEBUG, "History") << "Replay header:\n" << xdr::xdr_to_string(header);
    if (newLclHeader.hash != lh.hash)
    {
        mApplyLedgerFailureInvalidResultHash.Mark();
        throw std::runtime_error(
            fmt::format("replay of {:s} produced mismatched ledger hash {:s}",
                        LedgerManager::ledgerAbbrev(lh),
                        LedgerManager::ledgerAbbrev(newLclHeader)));
    }

    mApplyLedgerSuccess.Mark();
}

TxSetFramePtr
ApplyLedgerChainWork::getCurrentTxSet(LedgerHeaderHistoryEntry histEntry)
{
    auto seq = histEntry.header.ledgerSeq;
    do
    {
        if (mTxHistoryEntry.ledgerSeq < seq)
        {
            CLOG(DEBUG, "History")
                << "Skipping txset for ledger " << mTxHistoryEntry.ledgerSeq;
        }
        else if (mTxHistoryEntry.ledgerSeq > seq)
        {
            break;
        }
        else
        {
            CLOG(DEBUG, "History") << "Loaded txset for ledger " << seq;
            return std::make_shared<TxSetFrame>(mApp.getNetworkID(),
                                                mTxHistoryEntry.txSet);
        }
    } while (mTxIn && mTxIn.readOne(mTxHistoryEntry));

    CLOG(DEBUG, "History") << "Using empty txset for ledger " << seq;
    return std::make_shared<TxSetFrame>(histEntry.header.previousLedgerHash);
}

Work::State
ApplyLedgerChainWork::onSuccess()
{
    if (!mReady)
    {
        return WORK_PENDING;
    }

    mApp.getCatchupManager().logAndUpdateCatchupStatus(true);
    try
    {
        verifyMaybeApply();
    }
    catch (std::runtime_error& e)
    {
        CLOG(ERROR, "History") << "Verification or replay failed: " << e.what();
        return WORK_FAILURE_FATAL;
    }

    // Completed checkpoint or reached the end
    if (mCurrLedger == mRange.last() || mCurrLedger == mCurrCheckpoint)
    {
        return WORK_SUCCESS;
    }

    mCurrLedger += 1;
    return WORK_RUNNING;
}
}
