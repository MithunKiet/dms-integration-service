"""Job schedule definitions for the APScheduler-based scheduler."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ScheduleConfig:
    """Holds the scheduling parameters for a single job.

    Attributes:
        job_id: Unique identifier matching a constant in ``config.constants``.
        func_path: Dotted import path to the callable (e.g. ``"jobs.customer.run"``).
        trigger_type: Either ``"interval"`` or ``"cron"``.
        trigger_kwargs: Keyword arguments forwarded to the APScheduler trigger.
        max_instances: Maximum number of concurrent instances allowed.
        coalesce: Whether to coalesce missed runs into one.
        replace_existing: Whether to replace an existing job with the same id.
    """

    job_id: str
    func_path: str
    trigger_type: str
    trigger_kwargs: Dict[str, Any] = field(default_factory=dict)
    max_instances: int = 1
    coalesce: bool = True
    replace_existing: bool = True


def get_schedules() -> List[ScheduleConfig]:
    """Return the list of :class:`ScheduleConfig` objects for all registered jobs."""
    return [
        ScheduleConfig(
            job_id="hmis_to_dms_customer_sync",
            func_path="jobs.hmis_to_dms.customer.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 15},
        ),
        ScheduleConfig(
            job_id="hmis_to_dms_dealer_sync",
            func_path="jobs.hmis_to_dms.dealer.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 30},
        ),
        ScheduleConfig(
            job_id="hmis_to_dms_product_sync",
            func_path="jobs.hmis_to_dms.product.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 30},
        ),
        ScheduleConfig(
            job_id="hmis_to_dms_stock_sync",
            func_path="jobs.hmis_to_dms.stock.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 10},
        ),
        ScheduleConfig(
            job_id="hmis_to_dms_order_sync",
            func_path="jobs.hmis_to_dms.order.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 5},
        ),
        ScheduleConfig(
            job_id="hmis_to_dms_invoice_sync",
            func_path="jobs.hmis_to_dms.invoice.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 10},
        ),
        ScheduleConfig(
            job_id="dms_to_hmis_customer_sync",
            func_path="jobs.dms_to_hmis.customer.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 20},
        ),
        ScheduleConfig(
            job_id="dms_to_hmis_dealer_sync",
            func_path="jobs.dms_to_hmis.dealer.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 30},
        ),
        ScheduleConfig(
            job_id="dms_to_hmis_product_sync",
            func_path="jobs.dms_to_hmis.product.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 30},
        ),
        ScheduleConfig(
            job_id="dms_to_hmis_stock_sync",
            func_path="jobs.dms_to_hmis.stock.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 15},
        ),
        ScheduleConfig(
            job_id="dms_to_hmis_order_sync",
            func_path="jobs.dms_to_hmis.order.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 5},
        ),
        ScheduleConfig(
            job_id="dms_to_hmis_invoice_sync",
            func_path="jobs.dms_to_hmis.invoice.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 10},
        ),
        ScheduleConfig(
            job_id="health_check",
            func_path="jobs.system.health_check.run",
            trigger_type="interval",
            trigger_kwargs={"minutes": 1},
        ),
        ScheduleConfig(
            job_id="retry_failed_records",
            func_path="jobs.system.retry_failed.run",
            trigger_type="interval",
            trigger_kwargs={"hours": 1},
        ),
        ScheduleConfig(
            job_id="reconciliation",
            func_path="jobs.system.reconciliation.run",
            trigger_type="interval",
            trigger_kwargs={"hours": 6},
        ),
    ]
