"""样品冷链测温服务:固定阈值配置、带时区读数写入与越界区间重算。

规则
----
- 先为已有样品登记**固定**测温配置(允许温度上下限 min_temp/max_temp、
  最大采样间隔 max_interval_seconds);登记后不可改,同样品同内容重放回放,
  异内容返回 `409 temperature_profile_conflict`。
- 读数按样品写入,测温时间必须**带时区**,服务端归一化到 UTC 微秒时刻后
  作为幂等时刻:同样品同一归一化 UTC 时刻的同值重放不新增记录,
  异值返回 `409 temperature_reading_conflict`。读数只追加、不可变,
  **允许乱序补录**(UNIQUE(sample_id, measured_at_utc) 兜底)。
- 越界区间查询**依据当前全部读数重新计算**(不物化区间),因此补录可即时
  合并或拆分既有区间:按时间升序扫描,低于下限或高于上限的相邻读数仅在
  间隔不超过配置的最大采样间隔且方向一致时归为同一区间;正常读数或超间隔
  (或方向变化)截断区间。返回起止时间、越界方向与区间极值。

幂等
----
- 配置:同样品同 min/max/max_interval/note 重放返回首次结果(replayed=True),
  不同内容 409;触发器禁止 UPDATE/DELETE。
- 读数:同 (样品, 归一化 UTC 时刻) 同值重放返回首次结果(replayed=True),
  异值 409;触发器禁止 UPDATE/DELETE。
"""
from __future__ import annotations

import sqlite3
from datetime import timezone
from typing import Optional

from . import schemas as s


class TemperatureError(Exception):
    """冷链测温业务冲突,携带结构化冲突列表。"""

    def __init__(self, conflicts: list[s.Conflict]):
        self.conflicts = conflicts
        super().__init__(conflicts[0].message if conflicts else "temperature conflict")


def _conflict(
    code: str,
    message: str,
    *,
    sample_id: Optional[str] = None,
    requested: Optional[float] = None,
    available: Optional[float] = None,
) -> s.Conflict:
    return s.Conflict(
        code=code,
        message=message,
        sample_id=sample_id,
        requested=requested,
        available=available,
    )


# ---------------------------------------------------------------- 时间工具

def _to_utc(dt):
    """带时区 datetime 归一化为 UTC datetime。"""
    return dt.astimezone(timezone.utc)


def _utc_iso(dt_utc) -> str:
    """UTC 微秒精度 ISO8601(以 Z 结尾),定长格式可直接按字符串排序,
    也是归一化幂等时刻。"""
    s_iso = dt_utc.isoformat(timespec="microseconds")
    return s_iso[:-6] + "Z" if s_iso.endswith("+00:00") else s_iso


def _epoch(dt_utc) -> float:
    return dt_utc.timestamp()


def _sample_exists(conn: sqlite3.Connection, sample_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM samples WHERE sample_id = ?", (sample_id,)
    ).fetchone() is not None


# ---------------------------------------------------------------- 配置登记

def _get_profile(
    conn: sqlite3.Connection, sample_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM temperature_profiles WHERE sample_id = ?", (sample_id,)
    ).fetchone()


def _build_profile_result(row: sqlite3.Row, *, replayed: bool) -> s.TemperatureProfileResult:
    return s.TemperatureProfileResult(
        sample_id=row["sample_id"],
        min_temp=row["min_temp"],
        max_temp=row["max_temp"],
        max_interval_seconds=row["max_interval_seconds"],
        note=row["note"],
        replayed=replayed,
        created_at=row["created_at"],
    )


def register_profile(
    conn: sqlite3.Connection, sample_id: str, req: s.TemperatureProfileRequest
) -> s.TemperatureProfileResult:
    """为已有样品登记固定温度上下限与最大采样间隔(单事务,登记后不可改)。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        if not _sample_exists(conn, sample_id):
            raise TemperatureError([_conflict(
                "sample_not_found",
                f"样品 {sample_id} 不存在,无法登记冷链测温配置",
                sample_id=sample_id,
            )])
        existing = _get_profile(conn, sample_id)
        if existing is not None:
            same = (
                existing["min_temp"] == req.min_temp
                and existing["max_temp"] == req.max_temp
                and existing["max_interval_seconds"] == req.max_interval_seconds
                and existing["note"] == req.note
            )
            if not same:
                raise TemperatureError([_conflict(
                    "temperature_profile_conflict",
                    f"样品 {sample_id} 已登记测温配置(下限 {existing['min_temp']}、"
                    f"上限 {existing['max_temp']}、最大采样间隔 "
                    f"{existing['max_interval_seconds']} 秒),配置登记后固定不可改",
                    sample_id=sample_id,
                    requested=req.min_temp,
                    available=existing["min_temp"],
                )])
            result = _build_profile_result(existing, replayed=True)
            conn.execute("COMMIT")
            return result

        conn.execute(
            "INSERT INTO temperature_profiles (sample_id, min_temp, max_temp,"
            " max_interval_seconds, note) VALUES (?, ?, ?, ?, ?)",
            (sample_id, req.min_temp, req.max_temp,
             req.max_interval_seconds, req.note),
        )
        result = _build_profile_result(_get_profile(conn, sample_id), replayed=False)  # type: ignore[arg-type]
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


def get_profile(
    conn: sqlite3.Connection, sample_id: str
) -> s.TemperatureProfileResult:
    """查询样品测温配置;样品/配置不存在返回 404 类冲突。"""
    if not _sample_exists(conn, sample_id):
        raise TemperatureError([_conflict(
            "sample_not_found",
            f"样品 {sample_id} 不存在",
            sample_id=sample_id,
        )])
    row = _get_profile(conn, sample_id)
    if row is None:
        raise TemperatureError([_conflict(
            "temperature_profile_not_found",
            f"样品 {sample_id} 尚未登记冷链测温配置(允许温度上下限与最大采样间隔)",
            sample_id=sample_id,
        )])
    return _build_profile_result(row, replayed=False)


# ---------------------------------------------------------------- 读数写入

def _build_reading_result(row: sqlite3.Row, *, replayed: bool) -> s.TemperatureReadingResult:
    return s.TemperatureReadingResult(
        reading_id=row["reading_id"],
        sample_id=row["sample_id"],
        measured_at=row["measured_at_raw"],
        measured_at_utc=row["measured_at_utc"],
        value=row["value"],
        note=row["note"],
        created_at=row["created_at"],
        replayed=replayed,
    )


def add_reading(
    conn: sqlite3.Connection, sample_id: str, req: s.TemperatureReadingIn
) -> s.TemperatureReadingResult:
    """按样品写入一条带时区测温读数(单事务,只追加):
    同 (样品, 归一化 UTC 时刻) 同值重放不新增,异值 409。允许乱序补录。"""
    dt_utc = _to_utc(req.measured_at)
    at_utc = _utc_iso(dt_utc)
    epoch = _epoch(dt_utc)
    raw = req.measured_at.isoformat()

    conn.execute("BEGIN IMMEDIATE")
    try:
        if not _sample_exists(conn, sample_id):
            raise TemperatureError([_conflict(
                "sample_not_found",
                f"样品 {sample_id} 不存在,无法写入测温读数",
                sample_id=sample_id,
            )])
        existing = conn.execute(
            "SELECT * FROM temperature_readings"
            " WHERE sample_id = ? AND measured_at_utc = ?",
            (sample_id, at_utc),
        ).fetchone()
        if existing is not None:
            if existing["value"] != req.value:
                raise TemperatureError([_conflict(
                    "temperature_reading_conflict",
                    f"样品 {sample_id} 在归一化 UTC 时刻 {at_utc} 已存在测温值 "
                    f"{existing['value']},本次提交 {req.value} 与之不同;"
                    "同一时刻的读数不可更改",
                    sample_id=sample_id,
                    requested=req.value,
                    available=existing["value"],
                )])
            result = _build_reading_result(existing, replayed=True)
            conn.execute("COMMIT")
            return result

        cur = conn.execute(
            "INSERT INTO temperature_readings (sample_id, measured_at_utc,"
            " measured_at_raw, measured_at_epoch, value, note)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (sample_id, at_utc, raw, epoch, req.value, req.note),
        )
        row = conn.execute(
            "SELECT * FROM temperature_readings WHERE reading_id = ?",
            (cur.lastrowid,),
        ).fetchone()
        result = _build_reading_result(row, replayed=False)  # type: ignore[arg-type]
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------- 读数查询

def list_readings(
    conn: sqlite3.Connection, sample_id: str, order: str = "asc"
) -> s.TemperatureReadingListResponse:
    """按样品列出当前全部测温读数(默认时间升序);样品不存在抛 404 类冲突。"""
    if not _sample_exists(conn, sample_id):
        raise TemperatureError([_conflict(
            "sample_not_found",
            f"样品 {sample_id} 不存在",
            sample_id=sample_id,
        )])
    direction = "ASC" if order == "asc" else "DESC"
    rows = conn.execute(
        f"SELECT * FROM temperature_readings WHERE sample_id = ?"
        f" ORDER BY measured_at_epoch {direction}, reading_id {direction}",
        (sample_id,),
    ).fetchall()
    return s.TemperatureReadingListResponse(
        sample_id=sample_id,
        order=order,  # type: ignore[arg-type]
        count=len(rows),
        items=[
            s.TemperatureReadingItem(
                sample_id=r["sample_id"],
                measured_at=r["measured_at_raw"],
                measured_at_utc=r["measured_at_utc"],
                value=r["value"],
            )
            for r in rows
        ],
    )


# ---------------------------------------------------------------- 越界区间

def _direction(value: float, lo: float, hi: float) -> Optional[str]:
    if value < lo:
        return "low"
    if value > hi:
        return "high"
    return None


def compute_excursions(
    conn: sqlite3.Connection, sample_id: str
) -> s.TemperatureExcursionResponse:
    """依据当前全部读数重新计算越界区间。

    按时间升序扫描:仅相邻的同方向越界读数且间隔 <= max_interval_seconds
    才延续当前区间;正常读数、超间隔或方向变化都截断区间。极值并列取最早。
    """
    if not _sample_exists(conn, sample_id):
        raise TemperatureError([_conflict(
            "sample_not_found",
            f"样品 {sample_id} 不存在",
            sample_id=sample_id,
        )])
    profile = _get_profile(conn, sample_id)
    if profile is None:
        raise TemperatureError([_conflict(
            "temperature_profile_not_found",
            f"样品 {sample_id} 尚未登记冷链测温配置,无法判定越界区间",
            sample_id=sample_id,
        )])

    lo = profile["min_temp"]
    hi = profile["max_temp"]
    gap_limit = profile["max_interval_seconds"]

    rows = conn.execute(
        "SELECT * FROM temperature_readings WHERE sample_id = ?"
        " ORDER BY measured_at_epoch ASC, reading_id ASC",
        (sample_id,),
    ).fetchall()

    intervals: list[s.TemperatureExcursionOut] = []

    def _emit(direction: str, members: list[sqlite3.Row]) -> None:
        if direction == "low":
            extreme = min(members, key=lambda r: (r["value"], r["reading_id"]))
        else:
            extreme = min(members, key=lambda r: (-r["value"], r["reading_id"]))
        intervals.append(s.TemperatureExcursionOut(
            direction=direction,  # type: ignore[arg-type]
            start_at=members[0]["measured_at_utc"],
            end_at=members[-1]["measured_at_utc"],
            extreme_value=extreme["value"],
            extreme_at=extreme["measured_at_utc"],
            reading_count=len(members),
        ))

    cur_dir: Optional[str] = None
    members: list[sqlite3.Row] = []
    prev_epoch: Optional[float] = None
    for r in rows:
        d = _direction(r["value"], lo, hi)
        if d is None:
            # 正常读数截断当前区间
            if cur_dir is not None:
                _emit(cur_dir, members)
            cur_dir, members, prev_epoch = None, [], None
            continue
        if cur_dir is None:
            cur_dir, members, prev_epoch = d, [r], r["measured_at_epoch"]
            continue
        gap = r["measured_at_epoch"] - prev_epoch
        if d == cur_dir and gap <= gap_limit:
            # 同方向且相邻间隔不超过配置:归入同一区间
            members.append(r)
        else:
            # 超间隔或方向变化:截断旧区间,另起新区间
            _emit(cur_dir, members)
            cur_dir, members = d, [r]
        prev_epoch = r["measured_at_epoch"]
    if cur_dir is not None:
        _emit(cur_dir, members)

    return s.TemperatureExcursionResponse(
        sample_id=sample_id,
        min_temp=lo,
        max_temp=hi,
        max_interval_seconds=gap_limit,
        total_readings=len(rows),
        intervals=intervals,
    )
