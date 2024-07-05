from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import and_, desc
from typing_extensions import final
from app.commons import tracing
from app.commons.database.infra import DB, DBWithPrimary
from app.payout.repository.bankdb.base import PayoutBankDBRepository
from app.payout.repository.bankdb.model import transaction_segments
from app.payout.repository.bankdb.model.transaction_segment import (
    TransactionSegmentCreate,
    TransactionSegment,
)
from app.payout.repository.repo_controls import use_real_replica_for_bankdb_repo_method


class TransactionSegmentRepositoryInterface(ABC):
    @abstractmethod
    async def create_transaction_segment(
        self, data: TransactionSegmentCreate
    ) -> TransactionSegment:
        pass

    @abstractmethod
    async def update_transaction_segment_transfer_id_by_id(
        self, transfer_id: int, transaction_segment_id: int
    ) -> Optional[TransactionSegment]:
        pass

    @abstractmethod
    async def delete_transaction_segment_by_id(
        self, transaction_segment_id: int
    ) -> Optional[TransactionSegment]:
        pass

    @abstractmethod
    async def get_transaction_segment_by_id(
        self, transaction_segment_id: int
    ) -> Optional[TransactionSegment]:
        pass

    @abstractmethod
    async def get_transaction_segments_by_payment_account_id_and_transfer_id(
        self, payment_account_id: int, transfer_id: int
    ) -> List[TransactionSegment]:
        pass

    @abstractmethod
    async def get_transaction_segments_by_payment_account_id(
        self, payment_account_id: int
    ) -> List[TransactionSegment]:
        pass

    @abstractmethod
    async def get_latest_transaction_segments_by_payment_account_id(
        self, payment_account_id: int
    ) -> Optional[TransactionSegment]:
        pass

    @abstractmethod
    async def get_unlinked_transaction_segments_by_payment_account_id(
        self, payment_account_id: int
    ) -> List[TransactionSegment]:
        pass


@final
@tracing.track_breadcrumb(repository_name="transaction_segments")
class TransactionSegmentRepositoryTest(
    PayoutBankDBRepository, TransactionSegmentRepositoryInterface
):
    def __init__(self, database: DB, bankdb_crdb: Optional[DBWithPrimary] = None):
        super().__init__(_database=database, _bankdb_crdb=bankdb_crdb)

    async def create_transaction_segment(
        self, data: TransactionSegmentCreate
    ) -> TransactionSegment:
        now_time = datetime.now(timezone.utc)
        stmt = (
            transaction_segments.table.insert()
            .values(
                data.dict(skip_defaults=True), created_at=now_time, updated_at=now_time
            )
            .returning(*transaction_segments.table.columns.values())
        )
        row = await self._bankdb_crdb.primary().fetch_one(stmt)
        assert row is not None
        return TransactionSegment.from_row(row)

    async def update_transaction_segment_transfer_id_by_id(
        self, transfer_id: int, transaction_segment_id: int
    ) -> Optional[TransactionSegment]:
        stmt = (
            transaction_segments.table.update()
            .where(transaction_segments.id == transaction_segment_id)
            .values(transfer_id=transfer_id, updated_at=datetime.utcnow())
            .returning(*transaction_segments.table.columns.values())
        )
        row = await self._database.primary().fetch_one(stmt)
        return TransactionSegment.from_row(row) if row else None

    async def delete_transaction_segment_by_id(
        self, transaction_segment_id: int
    ) -> Optional[TransactionSegment]:
        stmt = (
            transaction_segments.table.update()
            .where(transaction_segments.id == transaction_segment_id)
            .values(deleted_at=datetime.utcnow())
            .returning(*transaction_segments.table.columns.values())
        )
        row = await self._database.primary().fetch_one(stmt)
        return TransactionSegment.from_row(row) if row else None

    async def get_transaction_segment_by_id(
        self, transaction_segment_id: int
    ) -> Optional[TransactionSegment]:
        stmt = transaction_segments.table.select().where(
            transaction_segments.id == transaction_segment_id
        )
        row = await self._bankdb_crdb.primary().fetch_one(stmt)
        return TransactionSegment.from_row(row) if row else None

    async def get_transaction_segments_by_payment_account_id_and_transfer_id(
        self, payment_account_id: int, transfer_id: int
    ) -> List[TransactionSegment]:
        stmt = transaction_segments.table.select().where(
            and_(
                transaction_segments.payment_account_id == payment_account_id,
                transaction_segments.transfer_id == transfer_id,
            )
        )
        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transaction_segment",
                method_name="get_transaction_segments_by_payment_account_id_and_transfer_id",
            )
        ).fetch_all(stmt)
        return [TransactionSegment.from_row(row) for row in rows]

    async def get_transaction_segments_by_payment_account_id(
        self, payment_account_id: int
    ) -> List[TransactionSegment]:
        stmt = (
            transaction_segments.table.select()
            .where(transaction_segments.payment_account_id == payment_account_id)
            .order_by(desc(transaction_segments.created_at))
        )

        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transaction_segment",
                method_name="get_transaction_segments_by_payment_account_id",
            )
        ).fetch_all(stmt)
        return [TransactionSegment.from_row(row) for row in rows]

    async def get_latest_transaction_segments_by_payment_account_id(
        self, payment_account_id: int
    ) -> Optional[TransactionSegment]:
        stmt = (
            transaction_segments.table.select()
            .where(transaction_segments.payment_account_id == payment_account_id)
            .order_by(desc(transaction_segments.start_transaction_id))
            .limit(1)
        )
        row = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transaction_segment",
                method_name="get_latest_transaction_segments_by_payment_account_id",
            )
        ).fetch_one(stmt)
        return TransactionSegment.from_row(row) if row else None

    async def get_unlinked_transaction_segments_by_payment_account_id(
        self, payment_account_id: int
    ) -> List[TransactionSegment]:
        stmt = transaction_segments.table.select().where(
            and_(
                transaction_segments.payment_account_id == payment_account_id,
                transaction_segments.transfer_id.is_(None),
            )
        )
        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transaction_segment",
                method_name="get_unlinked_transaction_segments_by_payment_account_id",
            )
        ).fetch_all(stmt)
        return [TransactionSegment.from_row(row) for row in rows]



transaction_segment_repo = TransactionSegmentRepositoryTest(
    database=app_context.payout_bankdb,
    bankdb_crdb=app_context.payout_bankdb_crdb,
)
log = app_context.log
created_tr_seg = await transaction_segment_repo.create_transaction_segment(
    data=TransactionSegmentCreate(
        amount = 20,
        start_transaction_id = 2,
        end_transaction_id = 2,
        payment_account_id = 0,
    )
)
log.info("created transaction_segment id: " + str(created_tr_seg.id))
fetched_tr_seg = await transaction_segment_repo.get_transaction_segment_by_id(
    transaction_segment_id=created_tr_seg.id
)
log.info("fetched transaction_segment id: " + str(fetched_tr_seg.id))
