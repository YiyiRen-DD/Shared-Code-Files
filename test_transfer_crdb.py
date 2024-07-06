from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import and_, asc, desc, func, select
from typing_extensions import final

from app.commons import tracing
from app.commons.database.infra import DB, DBWithPrimary
from app.payout.core.feature_flags import enable_transfer_query_optimizer_workaround
from app.payout.models import TransferMethodType
from app.payout.repository.bank_db.model import stripe_transfers, transfers
from app.payout.repository.base import PayoutDBRepository
from app.payout.repository.data_model.base.transfer import (
    Transfer,
    TransferCreate,
    TransferEntryWithCumulativeAmount,
    TransferStatus,
    TransferUpdate,
)
from app.payout.repository.interface import TransferRepositoryInterface
from app.payout.repository.repo_controls import use_real_replica_for_bankdb_repo_method
from datetime import datetime

from structlog.stdlib import BoundLogger

from app.payout.repository.base import DbConnectors, PayoutMultiDBRepository
from app.payout.repository.data_model.base.common import CurrencyAmount
from app.payout.repository.maindb.transfer import TransferMainDBRepository
from app.payout.repository.shadow_writer import (
    transfer_create_shadow_writer,
    transfer_update_shadow_writer,
)

from app.payout.repository.base import DbConnectors
from app.payout.models import PayoutAccountEntity, PayoutCountry, PayoutDay, Timezones
from app.payout.core.transfer.utils import get_last_week, start_and_end_of_date


@final
@tracing.track_breadcrumb(database_name="bankdb", repository_name="transfer")
class TransferBankDBRepositoryTest(PayoutDBRepository, TransferRepositoryInterface):
    def __init__(self, database: DB, bankdb_crdb: Optional[DBWithPrimary] = None):
        super().__init__(_database=database, _bankdb_crdb=bankdb_crdb)

    async def create_transfer(self, data: TransferCreate) -> Transfer:
        created_at = data.created_at or datetime.now(timezone.utc)
        stmt = (
            transfers.table.insert()
            .values(data.dict(skip_defaults=True), created_at=created_at)
            .returning(*transfers.table.columns.values())
        )
        if self._bankdb_crdb is None:
            print("it's none")
        row = await self._bankdb_crdb.primary().fetch_one(stmt)
        assert row is not None
        return Transfer.from_row(row)

    async def get_transfer_by_id(self, transfer_id: int) -> Optional[Transfer]:
        stmt = transfers.table.select().where(transfers.id == transfer_id)
        row = await self._bankdb_crdb.primary().fetch_one(stmt)
        return Transfer.from_row(row) if row else None

    async def get_transfers_by_ids(self, transfer_ids: List[int]) -> List[Transfer]:
        stmt = transfers.table.select().where(transfers.id.in_(transfer_ids))
        rows = await self._database.primary_or_replica().fetch_all(stmt)
        results = [Transfer.from_row(row) for row in rows] if rows else []
        return results

    async def get_transfers_by_submitted_at_and_method(
        self, start_time: datetime
    ) -> List[int]:
        override_stmt_timeout_in_ms = 600 * 1000
        query = and_(
            transfers.submitted_at.__ge__(start_time),
            transfers.method == TransferMethodType.STRIPE,
        )
        get_transfers_stmt = transfers.table.select().where(query)
        override_stmt_timeout_stmt = "SET LOCAL statement_timeout = {};".format(
            override_stmt_timeout_in_ms
        )
        async with self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_transfers_by_submitted_at_and_method",
            )
        ).transaction() as transaction:
            await transaction.connection().execute(override_stmt_timeout_stmt)
            rows = await transaction.connection().fetch_all(get_transfers_stmt)

        if rows:
            results = [Transfer.from_row(row) for row in rows]
            return [transfer.id for transfer in results]
        else:
            return []

    async def get_transfers_by_payment_account_ids_and_count(
        self, payment_account_ids: List[int], offset: int, limit: int
    ) -> Tuple[List[Transfer], int]:
        query = and_(
            transfers.payment_account_id.isnot(None),
            transfers.payment_account_id.in_(payment_account_ids),
        )
        get_transfers_stmt = (
            transfers.table.select()
            .where(query)
            .order_by(desc(transfers.id))
            .offset(offset)
            .limit(limit)
        )
        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_transfers_by_payment_account_ids_and_count",
            )
        ).fetch_all(get_transfers_stmt)
        results = [Transfer.from_row(row) for row in rows] if rows else []

        count_stmt = transfers.table.count().where(query)
        count = 0
        count_fetched = await self._database.primary_or_replica().fetch_value(
            count_stmt
        )
        if count_fetched:
            count = count_fetched
        return results, int(count)

    async def get_unsubmitted_transfer_ids(self, created_before: datetime) -> List[int]:
        query = and_(
            transfers.created_at.__lt__(created_before),
            transfers.amount.__ge__(0),
            transfers.status == TransferStatus.NEW,
        )
        stmt = transfers.table.select().where(query)
        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer", method_name="get_unsubmitted_transfer_ids"
            )
        ).fetch_all(stmt)

        if rows:
            results = [Transfer.from_row(row) for row in rows]
            return [transfer.id for transfer in results]
        else:
            return []

    async def get_transfers_and_count_by_status_and_time_range(
        self,
        has_positive_amount: bool,
        offset: int,
        limit: int,
        statuses: List[str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Tuple[List[Transfer], int]:
        override_stmt_timeout_in_ms = 300 * 1000

        query = and_(transfers.status.isnot(None), transfers.status.in_(statuses))
        if has_positive_amount:
            query.clauses.append(transfers.amount.__gt__(0))
        if start_time:
            query.clauses.append(transfers.created_at.__gt__(start_time))
        if end_time:
            query.clauses.append(transfers.created_at.__lt__(end_time))

        # TODO: may replace with cursor pagination for O(1) performance
        get_transfers_stmt = (
            transfers.table.select()
            .where(query)
            .order_by(desc(transfers.id))
            .offset(offset)
            .limit(limit)
        )

        if enable_transfer_query_optimizer_workaround():
            get_transfers_stmt = (
                transfers.table.select()
                .where(query)
                .order_by(desc(transfers.id))
                .order_by(
                    desc(transfers.created_at)
                )  # need it here to make sure optimizer pick the right index
                .offset(offset)
                .limit(limit)
            )

        count = 0
        override_stmt_timeout_stmt = "SET LOCAL statement_timeout = {};".format(
            override_stmt_timeout_in_ms
        )
        async with self._database.primary_or_replica_slow(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_transfers_and_count_by_status_and_time_range",
            )
        ).transaction() as transaction:
            await transaction.connection().execute(override_stmt_timeout_stmt)
            rows = await transaction.connection().fetch_all(get_transfers_stmt)
            results = [Transfer.from_row(row) for row in rows] if rows else []
            count_stmt = transfers.table.count().where(query)
            count_fetched = await transaction.connection().fetch_value(count_stmt)

        if count_fetched:
            count = count_fetched
        return results, int(count)

    async def get_positive_amount_transfers_and_count_by_time_range(
        self,
        offset: int,
        limit: int,
        is_submitted: bool,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Tuple[List[Transfer], int]:
        query = and_(transfers.amount.isnot(None), transfers.amount.__gt__(0))
        if is_submitted:
            query.clauses.append(stripe_transfers.table.c.transfer_id == transfers.id)
        if start_time:
            query.clauses.append(transfers.created_at.__gt__(start_time))
        if end_time:
            query.clauses.append(transfers.created_at.__lt__(end_time))

        get_transfers_stmt = (
            transfers.table.select()
            .where(query)
            .order_by(desc(transfers.id))
            .offset(offset)
            .limit(limit)
        )
        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_positive_amount_transfers_and_count_by_time_range",
            )
        ).fetch_all(get_transfers_stmt)
        results = [Transfer.from_row(row) for row in rows] if rows else []

        count_stmt = transfers.table.count().where(query)
        count = 0
        count_fetched = await self._database.primary_or_replica().fetch_value(
            count_stmt
        )
        if count_fetched:
            count = count_fetched
        return results, int(count)

    async def update_transfer_by_id(
        self, transfer_id: int, data: TransferUpdate
    ) -> Optional[Transfer]:
        stmt = (
            transfers.table.update()
            .where(transfers.id == transfer_id)
            .values(data.dict(skip_defaults=True))
            .returning(*transfers.table.columns.values())
        )
        row = await self._database.primary().fetch_one(stmt)
        return Transfer.from_row(row) if row else None

    async def get_total_transfer_amount_by_payout_account_id(
        self,
        payout_account_id: int,
        statuses: Optional[List[TransferStatus]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[CurrencyAmount]:

        conditions = [transfers.payment_account_id == payout_account_id]
        if statuses:
            conditions.append(transfers.status.in_(statuses))
        if start_time:
            conditions.append(transfers.created_at.__ge__(start_time))
        if end_time:
            conditions.append(transfers.created_at.__le__(end_time))

        stmt = (
            transfers.table.select()
            .with_only_columns(
                [transfers.currency, func.sum(transfers.amount).label("total_amount")]
            )
            .where(and_(*conditions))
            .group_by(transfers.currency)
            .order_by(asc(transfers.currency))
        )
        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_total_transfer_amount_by_payout_account_id",
            )
        ).fetch_all(stmt)
        if rows:
            return [CurrencyAmount.from_row(row) for row in rows]
        return []

    async def get_transfer_set_by_payment_account_and_status(
        self, payment_account_id: int, statuses: List[str]
    ) -> List[int]:
        query = and_(
            transfers.status.isnot(None),
            transfers.status.in_(statuses),
            transfers.payment_account_id == payment_account_id,
        )
        stmt = transfers.table.select().where(query)
        rows = await self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_transfer_set_by_payment_account_and_status",
            )
        ).fetch_all(stmt)

        if rows:
            results = [Transfer.from_row(row) for row in rows]
            return [transfer.id for transfer in results]
        else:
            return []

    async def get_unsubmitted_transfers_with_cumulative_amount(
        self,
        currency: Optional[str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> List[TransferEntryWithCumulativeAmount]:
        # set this query to 30 sec timeout
        override_stmt_timeout_in_ms = 30 * 1000

        where_condition = and_(
            transfers.amount.__ge__(0), transfers.status == TransferStatus.NEW,
        )
        if start_time:
            where_condition.clauses.append(transfers.created_at.__ge__(start_time))
        if end_time:
            where_condition.clauses.append(transfers.created_at.__le__(end_time))
        if currency:
            where_condition.clauses.append(transfers.currency == currency)

        selected_columns = [
            transfers.id,
            transfers.payment_account_id,
            transfers.currency,
            transfers.amount,
            func.sum(transfers.amount)
            .over(order_by=transfers.amount, rows=(None, 0))
            .label("cumulative_amount"),
        ]

        # SELECT
        #   transfer.id,
        #   transfer.payment_account_id,
        #   transfer.currency,
        #   transfer.amount,
        #   sum(transfer.amount) OVER (
        #       ORDER BY transfer.amount ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        # FROM transfer
        # WHERE transfer.amount >= :amount_1 AND transfer.status = :status_1
        stmt = (
            select(columns=selected_columns)
            .where(where_condition)
            .select_from(transfers.table)
        )
        override_stmt_timeout_stmt = "SET LOCAL statement_timeout = {};".format(
            override_stmt_timeout_in_ms
        )
        async with self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_unsubmitted_transfers_with_cumulative_amount",
            )
        ).transaction() as transaction:
            await transaction.connection().execute(override_stmt_timeout_stmt)
            rows = await transaction.connection().fetch_all(stmt)

        if rows:
            return [TransferEntryWithCumulativeAmount.from_row(row) for row in rows]
        else:
            return []

    async def get_delayed_transfers(
        self,
        currency: Optional[str],
        statuses: Optional[List[str]],
        status_codes: Optional[List[str]],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> List[Transfer]:
        # set this query to 30 sec timeout
        override_stmt_timeout_in_ms = 30 * 1000

        where_condition = and_(
            transfers.amount.__ge__(0),
            transfers.status.isnot(None),
            transfers.status.in_(statuses),
            transfers.status_code.in_(status_codes),
        )

        if start_time:
            where_condition.clauses.append(transfers.created_at.__ge__(start_time))
        if end_time:
            where_condition.clauses.append(transfers.created_at.__le__(end_time))
        if currency:
            where_condition.clauses.append(transfers.currency == currency)

        selected_columns = [
            transfers.id,
            transfers.payment_account_id,
            transfers.currency,
            transfers.amount,
            transfers.subtotal,
            transfers.adjustments,
            transfers.method,
            transfers.created_at,
        ]

        # SELECT
        #   transfer.id,
        #   transfer.payment_account_id,
        #   transfer.currency,
        #   transfer.amount,
        #   transfers.subtotal,
        #   transfers.adjustments,
        #   transfers.method,
        #   transfers.created_at,
        # FROM transfer
        # WHERE transfer.amount >= :amount_1 AND transfer.status = :status_1 AND transfer.status_code = :status_code_1
        stmt = (
            select(columns=selected_columns)
            .where(where_condition)
            .select_from(transfers.table)
        )
        override_stmt_timeout_stmt = "SET LOCAL statement_timeout = {};".format(
            override_stmt_timeout_in_ms
        )
        async with self._database.primary_or_replica(
            use_real_replica=use_real_replica_for_bankdb_repo_method(
                repo_name="transfer",
                method_name="get_delayed_transfers_with_cumulative_amount",
            )
        ).transaction() as transaction:
            await transaction.connection().execute(override_stmt_timeout_stmt)
            rows = await transaction.connection().fetch_all(stmt)

        if rows:
            return [Transfer.from_row(row) for row in rows]
        else:
            return []

CONNECTED_DB_REPO_WITH_ORDER_CUTOVER: List[Dict] = [
    {"name": "bankdb", "repo_klass": TransferBankDBRepositoryTest},
    {"name": "maindb", "repo_klass": TransferMainDBRepository},
]


def get_current_repo_connector_config():
    return CONNECTED_DB_REPO_WITH_ORDER_CUTOVER


@final
class TransferRepositoryTest(PayoutMultiDBRepository, TransferRepositoryInterface):
    def __init__(self, db_connectors: DbConnectors, logger: BoundLogger):
        super().__init__(
            db_connectors=db_connectors,
            logger=logger,
            ordered_repo_connector_config=get_current_repo_connector_config(),
        )

    async def create_transfer(self, data: TransferCreate) -> Transfer:
        return await transfer_create_shadow_writer(
            req=data.dict(skip_defaults=True),
            main_repo=self.ordered_connected_repo[0],
            shadow_repo=self.ordered_connected_repo[1],
            logger=self.logger,
        )

    async def get_transfer_by_id(self, transfer_id: int) -> Optional[Transfer]:
        return await self.ordered_connected_repo[0].get_transfer_by_id(transfer_id)

    async def get_transfers_by_ids(self, transfer_ids: List[int]) -> List[Transfer]:
        return await self.ordered_connected_repo[0].get_transfers_by_ids(transfer_ids)

    async def get_transfers_by_submitted_at_and_method(
        self, start_time: datetime
    ) -> List[int]:
        return await self.ordered_connected_repo[
            0
        ].get_transfers_by_submitted_at_and_method(start_time)

    async def get_transfers_by_payment_account_ids_and_count(
        self, payment_account_ids: List[int], offset: int, limit: int
    ) -> Tuple[List[Transfer], int]:
        return await self.ordered_connected_repo[
            0
        ].get_transfers_by_payment_account_ids_and_count(
            payment_account_ids, offset, limit
        )

    async def get_unsubmitted_transfer_ids(self, created_before: datetime) -> List[int]:
        return await self.ordered_connected_repo[0].get_unsubmitted_transfer_ids(
            created_before
        )

    async def get_transfers_and_count_by_status_and_time_range(
        self,
        has_positive_amount: bool,
        offset: int,
        limit: int,
        statuses: List[str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Tuple[List[Transfer], int]:
        return await self.ordered_connected_repo[
            0
        ].get_transfers_and_count_by_status_and_time_range(
            has_positive_amount, offset, limit, statuses, start_time, end_time
        )

    async def get_positive_amount_transfers_and_count_by_time_range(
        self,
        offset: int,
        limit: int,
        is_submitted: bool,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Tuple[List[Transfer], int]:
        return await self.ordered_connected_repo[
            0
        ].get_positive_amount_transfers_and_count_by_time_range(
            offset, limit, is_submitted, start_time, end_time
        )

    async def update_transfer_by_id(
        self, transfer_id: int, data: TransferUpdate
    ) -> Optional[Transfer]:
        return await transfer_update_shadow_writer(
            req=data.dict(skip_defaults=True),
            main_repo=self.ordered_connected_repo[0],
            shadow_repo=self.ordered_connected_repo[1],
            logger=self.logger,
            transfer_id=transfer_id,
        )

    async def get_total_transfer_amount_by_payout_account_id(
        self,
        payout_account_id: int,
        statuses: Optional[List[TransferStatus]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[CurrencyAmount]:
        return await self.ordered_connected_repo[
            0
        ].get_total_transfer_amount_by_payout_account_id(
            payout_account_id, statuses, start_time, end_time
        )

    async def get_transfer_set_by_payment_account_and_status(
        self, payment_account_id: int, statuses: List[str]
    ) -> List[int]:
        return await self.ordered_connected_repo[
            0
        ].get_transfer_set_by_payment_account_and_status(payment_account_id, statuses)

    async def get_unsubmitted_transfers_with_cumulative_amount(
        self,
        currency: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[TransferEntryWithCumulativeAmount]:
        return await self.ordered_connected_repo[
            0
        ].get_unsubmitted_transfers_with_cumulative_amount(
            currency, start_time, end_time
        )

    async def get_delayed_transfers(
        self,
        currency: Optional[str] = None,
        statuses: Optional[List[str]] = None,
        status_codes: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Transfer]:
        return await self.ordered_connected_repo[0].get_delayed_transfers(
            currency, statuses, status_codes, start_time, end_time
        )


transfer_repo = TransferRepositoryTest(
    db_connectors=DbConnectors(
        {"maindb": app_context.payout_maindb, "bankdb": app_context.payout_bankdb, "bankdb_crdb": app_context.payout_bankdb_crdb}
    ),
    logger=app_context.log,
)
currency = 'USD' # revise currency here
manual_transfer_reason = "pr staging testing" # type reason for manually create transfer
payment_account_ids="""0""".split()
payout_country_timezone = Timezones.US_PACIFIC

start_time, end_time = get_last_week(
    timezone_info=payout_country_timezone, inclusive_end=True
)

for payment_account_id in payment_account_ids:
    
    subtotal = 20
    # only create transfer if subtotal >= 0 and we have unpaid transactions
    if subtotal >= 0:
        create_request = TransferCreate(
            payment_account_id=payment_account_id,
            subtotal=subtotal,
            amount=subtotal,
            adjustments="{}",
            method="",
            currency=currency,
            status=TransferStatus.NEW,
            manual_transfer_reason=manual_transfer_reason
        )
        transfer = await transfer_repo.create_transfer(data=create_request)
        print("created transfer id: " + str(transfer.id))

        fetched_transfer = await transfer_repo.get_transfer_by_id(transfer_id=transfer.id)
        print("fetched transfer id: " + str(fetched_transfer.id))

