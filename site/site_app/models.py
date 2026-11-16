from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Case(Base):
    """Local case registry. Case-level rows never leave this service."""

    __tablename__ = "cases"
    __table_args__ = (
        CheckConstraint("days > 0", name="ck_cases_days_positive"),
        CheckConstraint("event IN ('death','censored')", name="ck_cases_event"),
    )

    case_no: Mapped[str] = mapped_column(String(64), primary_key=True)
    days: Mapped[int] = mapped_column(Integer, nullable=False)  # positive follow-up days
    event: Mapped[str] = mapped_column(String(16), nullable=False)  # 'death' | 'censored'


class Snapshot(Base):
    """A frozen, aggregate-only view of `cases` bound to one computation round."""

    __tablename__ = "snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    round_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class SnapshotHist(Base):
    """Day-level aggregate histogram backing a snapshot (no case-level data)."""

    __tablename__ = "snapshot_hist"

    snapshot_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    day: Mapped[int] = mapped_column(Integer, primary_key=True)
    deaths: Mapped[int] = mapped_column(Integer, nullable=False)
    censored: Mapped[int] = mapped_column(Integer, nullable=False)


class FinalizedRound(Base):
    """Rounds that reached a terminal state; late snapshot requests are rejected."""

    __tablename__ = "finalized_rounds"

    round_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    finalized_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
