#pragma once
#include "columns_set.h"
#include "context.h"
#include "fetched_data.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <util/string/join.h>

namespace NKikimr::NOlap {
class IDataReader;
}

namespace NKikimr::NOlap::NReader::NPlain {

class TFetchingInterval;
class TPlainReadData;
class IFetchTaskConstructor;
class IFetchingStep;

class IDataSource {
private:
    YDB_ACCESSOR(bool, ExclusiveIntervalOnly, true);
    YDB_READONLY(ui32, SourceIdx, 0);
    YDB_READONLY_DEF(NArrow::NMerger::TSortableBatchPosition, Start);
    YDB_READONLY_DEF(NArrow::NMerger::TSortableBatchPosition, Finish);
    NArrow::TReplaceKey StartReplaceKey;
    NArrow::TReplaceKey FinishReplaceKey;
    YDB_READONLY_DEF(std::shared_ptr<TSpecialReadContext>, Context);
    YDB_READONLY(TSnapshot, RecordSnapshotMin, TSnapshot::Zero());
    YDB_READONLY(TSnapshot, RecordSnapshotMax, TSnapshot::Zero());
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(std::optional<ui64>, ShardingVersionOptional);
    YDB_READONLY(bool, HasDeletions, false);
    YDB_READONLY(ui32, IntervalsCount, 0);
    virtual NJson::TJsonValue DoDebugJson() const = 0;
    bool MergingStartedFlag = false;
    TAtomic SourceStartedFlag = 0;
    std::shared_ptr<TFetchingScript> FetchingPlan;
    std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> ResourceGuards;
    std::optional<ui64> FirstIntervalId;
    ui32 CurrentPlanStepIndex = 0;
    YDB_READONLY(TPKRangeFilter::EUsageClass, UsageClass, TPKRangeFilter::EUsageClass::PartialUsage);

protected:
    bool IsSourceInMemoryFlag = true;
    THashMap<ui32, TFetchingInterval*> Intervals;

    std::unique_ptr<TFetchedData> StageData;
    std::unique_ptr<TFetchedResult> StageResult;

    TAtomic FilterStageFlag = 0;
    bool IsReadyFlag = false;

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) = 0;
    virtual bool DoStartFetchingIndexes(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const std::shared_ptr<TIndexesSet>& indexes) = 0;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) = 0;
    virtual void DoAbort() = 0;
    virtual void DoApplyIndex(const NIndexes::TIndexCheckerContainer& indexMeta) = 0;
    virtual bool DoAddSequentialEntityIds(const ui32 entityId) = 0;
    virtual NJson::TJsonValue DoDebugJsonForMemory() const {
        return NJson::JSON_MAP;
    }
    virtual bool DoAddTxConflict() = 0;

public:
    bool AddTxConflict() {
        if (!Context->GetCommonContext()->HasLock()) {
            return false;
        }
        if (DoAddTxConflict()) {
            StageData->Clear();
            return true;
        }
        return false;
    }

    ui64 GetResourceGuardsMemory() const {
        ui64 result = 0;
        for (auto&& i : ResourceGuards) {
            result += i->GetMemory();
        }
        return result;
    }

    void RegisterAllocationGuard(const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard) {
        ResourceGuards.emplace_back(guard);
    }

    bool IsSourceInMemory() const {
        return IsSourceInMemoryFlag;
    }
    void SetFirstIntervalId(const ui64 value) {
        AFL_VERIFY(!FirstIntervalId);
        FirstIntervalId = value;
    }
    ui64 GetFirstIntervalId() const {
        AFL_VERIFY(!!FirstIntervalId);
        return *FirstIntervalId;
    }
    virtual bool IsSourceInMemory(const std::set<ui32>& fieldIds) const = 0;
    bool AddSequentialEntityIds(const ui32 entityId) {
        if (DoAddSequentialEntityIds(entityId)) {
            IsSourceInMemoryFlag = false;
            return true;
        }
        return false;
    }
    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const = 0;

    virtual ui64 GetPathId() const = 0;
    virtual bool HasIndexes(const std::set<ui32>& indexIds) const = 0;

    const NArrow::TReplaceKey& GetStartReplaceKey() const {
        return StartReplaceKey;
    }
    const NArrow::TReplaceKey& GetFinishReplaceKey() const {
        return FinishReplaceKey;
    }

    const TFetchedResult& GetStageResult() const {
        AFL_VERIFY(!!StageResult);
        return *StageResult;
    }

    void SetIsReady();

    void Finalize() {
        TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        StageResult = std::make_unique<TFetchedResult>(std::move(StageData));
    }

    void ApplyIndex(const NIndexes::TIndexCheckerContainer& indexMeta) {
        return DoApplyIndex(indexMeta);
    }

    void AssembleColumns(const std::shared_ptr<TColumnsSet>& columns) {
        if (columns->IsEmpty()) {
            return;
        }
        DoAssembleColumns(columns);
    }

    bool StartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) {
        return DoStartFetchingColumns(sourcePtr, step, columns);
    }

    bool StartFetchingIndexes(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const std::shared_ptr<TIndexesSet>& indexes) {
        AFL_VERIFY(indexes);
        return DoStartFetchingIndexes(sourcePtr, step, indexes);
    }
    void InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching);

    std::shared_ptr<arrow::RecordBatch> GetLastPK() const {
        return Finish.BuildSortingCursor().ExtractSortingPosition(Finish.GetSortFields());
    }
    void IncIntervalsCount() {
        ++IntervalsCount;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& columnIds) const = 0;
    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const = 0;
    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& columnsIds) const = 0;

    bool IsMergingStarted() const {
        return MergingStartedFlag;
    }

    void StartMerging() {
        AFL_VERIFY(!MergingStartedFlag);
        MergingStartedFlag = true;
    }

    void Abort() {
        Intervals.clear();
        DoAbort();
    }

    NJson::TJsonValue DebugJsonForMemory() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("details", DoDebugJsonForMemory());
        result.InsertValue("count", RecordsCount);
        return result;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("source_idx", SourceIdx);
        result.InsertValue("start", Start.DebugJson());
        result.InsertValue("finish", Finish.DebugJson());
        result.InsertValue("specific", DoDebugJson());
        return result;
    }

    bool OnIntervalFinished(const ui32 intervalIdx);

    bool IsDataReady() const {
        return IsReadyFlag;
    }

    void OnEmptyStageData() {
        if (!ResourceGuards.size()) {
            return;
        }
        if (ExclusiveIntervalOnly) {
            ResourceGuards.back()->Update(0);
        } else {
            ResourceGuards.back()->Update(GetColumnRawBytes(Context->GetPKColumns()->GetColumnIds()));
        }
    }

    const TFetchedData& GetStageData() const {
        AFL_VERIFY(StageData);
        return *StageData;
    }

    TFetchedData& MutableStageData() {
        AFL_VERIFY(StageData);
        return *StageData;
    }

    void RegisterInterval(TFetchingInterval& interval, const std::shared_ptr<IDataSource>& sourcePtr);

    IDataSource(const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context, const NArrow::TReplaceKey& start,
        const NArrow::TReplaceKey& finish, const TSnapshot& recordSnapshotMin, const TSnapshot& recordSnapshotMax, const ui32 recordsCount,
        const std::optional<ui64> shardingVersion, const bool hasDeletions)
        : SourceIdx(sourceIdx)
        , Start(context->GetReadMetadata()->BuildSortedPosition(start))
        , Finish(context->GetReadMetadata()->BuildSortedPosition(finish))
        , StartReplaceKey(start)
        , FinishReplaceKey(finish)
        , Context(context)
        , RecordSnapshotMin(recordSnapshotMin)
        , RecordSnapshotMax(recordSnapshotMax)
        , RecordsCount(recordsCount)
        , ShardingVersionOptional(shardingVersion)
        , HasDeletions(hasDeletions) {
        UsageClass = Context->GetReadMetadata()->GetPKRangesFilter().IsPortionInPartialUsage(GetStartReplaceKey(), GetFinishReplaceKey());
        AFL_VERIFY(UsageClass != TPKRangeFilter::EUsageClass::DontUsage);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portions_for_merge")("start", Start.DebugJson())("finish", Finish.DebugJson());
        if (Start.IsReverseSort()) {
            std::swap(Start, Finish);
        }
        Y_ABORT_UNLESS(Start.Compare(Finish) != std::partial_ordering::greater);
    }

    virtual ~IDataSource() {
        AFL_VERIFY(Intervals.empty());
    }
};

class TPortionDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    std::set<ui32> SequentialEntityIds;
    TPortionDataAccessor Portion;
    std::shared_ptr<ISnapshotSchema> Schema;
    mutable THashMap<ui64, ui64> FingerprintedData;

    void NeedFetchColumns(const std::set<ui32>& columnIds, TBlobsAction& blobsAction,
        THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>& nullBlocks, const std::shared_ptr<NArrow::TColumnFilter>& filter);

    virtual void DoApplyIndex(const NIndexes::TIndexCheckerContainer& indexChecker) override;
    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) override;
    virtual bool DoStartFetchingIndexes(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const std::shared_ptr<TIndexesSet>& indexes) override;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) override;
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "portion");
        result.InsertValue("info", Portion.GetPortionInfo().DebugString());
        result.InsertValue("commit", Portion.GetPortionInfo().GetCommitSnapshotOptional().value_or(TSnapshot::Zero()).DebugString());
        result.InsertValue("insert", (ui64)Portion.GetPortionInfo().GetInsertWriteIdOptional().value_or(TInsertWriteId(0)));
        return result;
    }

    virtual NJson::TJsonValue DoDebugJsonForMemory() const override {
        NJson::TJsonValue result = TBase::DoDebugJsonForMemory();
        auto columns = Portion.GetColumnIds();
        for (auto&& i : SequentialEntityIds) {
            AFL_VERIFY(columns.erase(i));
        }
        //        result.InsertValue("sequential_columns", JoinSeq(",", SequentialEntityIds));
        if (SequentialEntityIds.size()) {
            result.InsertValue("min_memory_seq", Portion.GetMinMemoryForReadColumns(SequentialEntityIds));
            result.InsertValue("min_memory_seq_blobs", Portion.GetColumnBlobBytes(SequentialEntityIds));
            result.InsertValue("in_mem", Portion.GetColumnRawBytes(columns, false));
        }
        result.InsertValue("columns_in_mem", JoinSeq(",", columns));
        result.InsertValue("portion_id", Portion.GetPortionInfo().GetPortionId());
        result.InsertValue("raw", Portion.GetPortionInfo().GetTotalRawBytes());
        result.InsertValue("blob", Portion.GetPortionInfo().GetTotalBlobBytes());
        result.InsertValue("read_memory", GetColumnRawBytes(Portion.GetColumnIds()));
        return result;
    }
    virtual void DoAbort() override;
    virtual ui64 GetPathId() const override {
        return Portion.GetPortionInfo().GetPathId();
    }
    virtual bool DoAddSequentialEntityIds(const ui32 entityId) override {
        FingerprintedData.clear();
        return SequentialEntityIds.emplace(entityId).second;
    }

public:
    virtual bool DoAddTxConflict() override {
        if (Portion.GetPortionInfo().HasCommitSnapshot() || !Portion.GetPortionInfo().HasInsertWriteId()) {
            GetContext()->GetReadMetadata()->SetBrokenWithCommitted();
            return true;
        } else if (!GetContext()->GetReadMetadata()->IsMyUncommitted(Portion.GetPortionInfo().GetInsertWriteIdVerified())) {
            GetContext()->GetReadMetadata()->SetConflictedWriteId(Portion.GetPortionInfo().GetInsertWriteIdVerified());
            return true;
        }
        return false;
    }

    virtual bool HasIndexes(const std::set<ui32>& indexIds) const override {
        return Portion.HasIndexes(indexIds);
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const override {
        return Portion.DecodeBlobAddresses(std::move(blobsOriginal), Schema->GetIndexInfo());
    }

    virtual bool IsSourceInMemory(const std::set<ui32>& fieldIds) const override {
        for (auto&& i : SequentialEntityIds) {
            if (fieldIds.contains(i)) {
                return false;
            }
        }
        return true;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& columnsIds) const override {
        AFL_VERIFY(columnsIds.size());
        const ui64 fp = CombineHashes(*columnsIds.begin(), *columnsIds.rbegin());
        auto it = FingerprintedData.find(fp);
        if (it != FingerprintedData.end()) {
            return it->second;
        }
        ui64 result = 0;
        if (SequentialEntityIds.size()) {
            std::set<ui32> selectedSeq;
            std::set<ui32> selectedInMem;
            for (auto&& i : columnsIds) {
                if (SequentialEntityIds.contains(i)) {
                    selectedSeq.emplace(i);
                } else {
                    selectedInMem.emplace(i);
                }
            }
            result = Portion.GetMinMemoryForReadColumns(selectedSeq) +
                     Portion.GetColumnBlobBytes(selectedSeq, false) +
                     Portion.GetColumnRawBytes(selectedInMem, false);
        } else {
            result = Portion.GetColumnRawBytes(columnsIds, false);
        }
        FingerprintedData.emplace(fp, result);
        return result;
    }

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& columnsIds) const override {
        return Portion.GetColumnBlobBytes(columnsIds, false);
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const override {
        return Portion.GetIndexRawBytes(indexIds, false);
    }

    const TPortionInfo& GetPortionInfo() const {
        return Portion.GetPortionInfo();
    }

    const TPortionInfo::TConstPtr& GetPortionInfoPtr() const {
        return Portion.GetPortionInfoPtr();
    }

    TPortionDataSource(const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase(sourceIdx, context, portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->RecordSnapshotMin(TSnapshot::Zero()),
              portion->RecordSnapshotMax(TSnapshot::Zero()),
              portion->GetRecordsCount(), portion->GetShardingVersionOptional(), portion->GetMeta().GetDeletionsCount())
        , Portion(portion)
        , Schema(GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*portion)) {
    }
};

class TCommittedDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    TCommittedBlob CommittedBlob;
    bool ReadStarted = false;

    virtual void DoAbort() override {
    }

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) override;
    virtual bool DoStartFetchingIndexes(const std::shared_ptr<IDataSource>& /*sourcePtr*/, const TFetchingScriptCursor& /*step*/,
        const std::shared_ptr<TIndexesSet>& /*indexes*/) override {
        return false;
    }
    virtual void DoApplyIndex(const NIndexes::TIndexCheckerContainer& /*indexMeta*/) override {
        return;
    }

    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) override;
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "commit");
        result.InsertValue("info", CommittedBlob.DebugString());
        return result;
    }
    virtual ui64 GetPathId() const override {
        return 0;
    }
    virtual bool DoAddSequentialEntityIds(const ui32 /*entityId*/) override {
        return false;
    }

    virtual bool DoAddTxConflict() override {
        if (CommittedBlob.IsCommitted()) {
            GetContext()->GetReadMetadata()->SetBrokenWithCommitted();
            return true;
        } else if (!GetContext()->GetReadMetadata()->IsMyUncommitted(CommittedBlob.GetInsertWriteId())) {
            GetContext()->GetReadMetadata()->SetConflictedWriteId(CommittedBlob.GetInsertWriteId());
            return true;
        }
        return false;
    }

public:
    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const override {
        THashMap<TChunkAddress, TString> result;
        for (auto&& i : blobsOriginal) {
            for (auto&& b : i.second) {
                result.emplace(TChunkAddress(1, 1), std::move(b.second));
            }
        }
        return result;
    }

    virtual bool IsSourceInMemory(const std::set<ui32>& /*fieldIds*/) const override {
        return true;
    }

    virtual bool HasIndexes(const std::set<ui32>& /*indexIds*/) const override {
        return false;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& /*columnIds*/) const override {
        return CommittedBlob.GetBlobRange().Size;
    }

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return CommittedBlob.GetBlobRange().Size;
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& /*columnIds*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    const TCommittedBlob& GetCommitted() const {
        return CommittedBlob;
    }

    TCommittedDataSource(const ui32 sourceIdx, const TCommittedBlob& committed, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase(sourceIdx, context, committed.GetFirst(), committed.GetLast(), committed.GetCommittedSnapshotDef(TSnapshot::Zero()),
              committed.GetCommittedSnapshotDef(TSnapshot::Zero()), committed.GetRecordsCount(), {}, committed.GetIsDelete())
        , CommittedBlob(committed) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NPlain
