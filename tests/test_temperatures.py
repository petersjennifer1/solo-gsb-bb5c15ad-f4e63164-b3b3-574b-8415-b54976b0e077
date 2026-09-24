"""样品冷链测温模块端到端测试:

- 为已有样品登记固定温度上下限与最大采样间隔:同内容重放、异内容 409、登记后不可变
- 读数写入:测温时间必须带时区(422),归一化 UTC 时刻为幂等键
  (不同时区拼写的同一时刻视为同一时刻:同值重放不新增、异值 409),允许乱序补录
- 越界区间:相邻同方向越界读数仅在间隔不超过配置值时归为同一区间,
  正常读数 / 超间隔 / 方向变化截断;返回起止时间、方向与极值(并列取最早)
- 乱序补录后查询依据当前全部读数重新计算:补录可合并或拆分既有区间
- 配置与读数审计不可变(触发器禁止 UPDATE/DELETE);并发由 BEGIN IMMEDIATE 串行化;
  旧库启动自动建表
"""
from __future__ import annotations

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app import temperatures as tp
from app.db import connect, init_db
from app.main import app, get_conn


@pytest.fixture()
def client():
    mem = connect(":memory:")
    init_db(mem)

    def _override():
        yield mem

    app.dependency_overrides[get_conn] = _override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
    mem.close()


def _sample(client, sid="S1"):
    r = client.post("/api/v1/operations/sampling", json={
        "type": "sampling", "op_no": f"OP-{sid}", "sample_id": sid,
        "material": "血清", "amount": 10, "unit": "mL",
    })
    assert r.status_code == 200, r.text


def _profile(client, sid="S1", lo=2.0, hi=8.0, gap=300, **extra):
    return client.put(f"/api/v1/samples/{sid}/temperature-profile", json={
        "min_temp": lo, "max_temp": hi, "max_interval_seconds": gap, **extra,
    })


def _reading(client, t, v, sid="S1", **extra):
    return client.post(
        f"/api/v1/samples/{sid}/temperature-readings",
        json={"measured_at": t, "value": v, **extra},
    )


def _excursions(client, sid="S1"):
    return client.get(f"/api/v1/samples/{sid}/temperature-excursions")


# ---------------------------------------------------------------- 配置登记

def test_register_profile_and_get(client):
    _sample(client)
    r = _profile(client, note="冷链试剂")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body == {
        "sample_id": "S1", "min_temp": 2.0, "max_temp": 8.0,
        "max_interval_seconds": 300.0, "note": "冷链试剂",
        "replayed": False, "created_at": body["created_at"],
    }
    g = client.get("/api/v1/samples/S1/temperature-profile")
    assert g.status_code == 200
    assert g.json()["min_temp"] == 2.0 and g.json()["replayed"] is False


def test_register_profile_for_missing_sample_conflicts(client):
    r = _profile(client, "GHOST")
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "sample_not_found"


def test_profile_same_content_replays_different_conflicts(client):
    _sample(client)
    assert _profile(client).json()["replayed"] is False
    assert _profile(client).json()["replayed"] is True
    r = _profile(client, lo=1.0)
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "temperature_profile_conflict"
    # 冲突后原配置不变
    g = client.get("/api/v1/samples/S1/temperature-profile").json()
    assert g["min_temp"] == 2.0 and g["max_temp"] == 8.0


def test_profile_invalid_bounds_and_interval_are_422(client):
    _sample(client)
    assert _profile(client, lo=8.0, hi=2.0).status_code == 422
    assert _profile(client, lo=8.0, hi=8.0).status_code == 422
    assert _profile(client, gap=0).status_code == 422
    assert _profile(client, gap=-1).status_code == 422


def test_get_profile_not_found_is_404(client):
    _sample(client)
    assert client.get("/api/v1/samples/GHOST/temperature-profile").status_code == 404
    r = client.get("/api/v1/samples/S1/temperature-profile")
    assert r.status_code == 404
    assert r.json()["error"] == "temperature_profile_not_found"


# ---------------------------------------------------------------- 读数写入

def test_add_reading_normalizes_timezone_and_persists(client):
    _sample(client); _profile(client)
    r = _reading(client, "2026-09-24T10:00:00+08:00", 5.0, note="探头A")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["replayed"] is False
    assert body["measured_at"] == "2026-09-24T10:00:00+08:00"
    assert body["measured_at_utc"] == "2026-09-24T02:00:00.000000Z"
    assert body["value"] == 5.0 and body["note"] == "探头A"
    assert isinstance(body["reading_id"], int)


def test_naive_measured_at_is_422(client):
    _sample(client); _profile(client)
    r = _reading(client, "2026-09-24T10:00:00", 5.0)
    assert r.status_code == 422
    assert "时区" in r.text


def test_reading_for_missing_sample_conflicts(client):
    r = _reading(client, "2026-09-24T10:00:00+08:00", 5.0, sid="GHOST")
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "sample_not_found"


def test_same_instant_same_value_replay_different_value_conflict(client):
    _sample(client); _profile(client)
    b1 = _reading(client, "2026-09-24T10:00:00+08:00", 9.0)
    assert b1.json()["replayed"] is False
    # 不同时区拼写但同一 UTC 时刻、同值 -> 重放,不新增
    b2 = _reading(client, "2026-09-24T02:00:00Z", 9.0)
    assert b2.status_code == 200
    assert b2.json()["replayed"] is True
    assert b2.json()["reading_id"] == b1.json()["reading_id"]
    # 同 UTC 时刻异值 -> 409,原值保留
    b3 = _reading(client, "2026-09-24T11:00:00+09:00", 1.0)  # = 02:00 UTC
    assert b3.status_code == 409
    c = b3.json()["conflicts"][0]
    assert c["code"] == "temperature_reading_conflict"
    assert c["sample_id"] == "S1"
    assert c["requested"] == 1.0 and c["available"] == 9.0
    rows = client.get("/api/v1/samples/S1/temperature-readings").json()
    assert rows["count"] == 1 and rows["items"][0]["value"] == 9.0


def test_out_of_order_backfill_is_accepted(client):
    _sample(client); _profile(client)
    assert _reading(client, "2026-09-24T10:05:00+08:00", 5.0).status_code == 200
    # 更早时刻补录
    assert _reading(client, "2026-09-24T10:00:00+08:00", 5.0).status_code == 200
    rows = client.get("/api/v1/samples/S1/temperature-readings").json()
    assert [i["measured_at_utc"] for i in rows["items"]] == [
        "2026-09-24T02:00:00.000000Z", "2026-09-24T02:05:00.000000Z",
    ]
    desc = client.get(
        "/api/v1/samples/S1/temperature-readings?order=desc"
    ).json()
    assert desc["order"] == "desc"
    assert desc["items"][0]["measured_at_utc"] == "2026-09-24T02:05:00.000000Z"
    assert client.get(
        "/api/v1/samples/S1/temperature-readings?order=sideways"
    ).status_code == 422


# ---------------------------------------------------------------- 越界区间

def test_excursions_group_split_by_gap_direction_and_normal(client):
    _sample(client)
    _profile(client, lo=2.0, hi=8.0, gap=300)
    plan = [
        ("2026-09-24T10:00:00+08:00", 9.0),   # high 起
        ("2026-09-24T10:04:00+08:00", 9.5),   # 240s <=300 同区间,极值 9.5
        ("2026-09-24T10:10:00+08:00", 9.0),   # 360s >300 截断,新 high
        ("2026-09-24T10:14:00+08:00", 1.0),   # 方向变化 -> 新 low
        ("2026-09-24T10:18:00+08:00", 1.2),   # 240s 同 low
        ("2026-09-24T10:22:00+08:00", 5.0),   # 正常截断
        ("2026-09-24T10:27:00+08:00", 0.5),   # 300s 后,新 low(正常读数已截断)
    ]
    for t, v in plan:
        assert _reading(client, t, v).status_code == 200
    r = _excursions(client)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total_readings"] == 7
    assert body["min_temp"] == 2.0 and body["max_temp"] == 8.0
    assert body["max_interval_seconds"] == 300.0
    iv = body["intervals"]
    assert [(i["direction"], i["reading_count"], i["extreme_value"])
            for i in iv] == [
        ("high", 2, 9.5), ("high", 1, 9.0),
        ("low", 2, 1.0), ("low", 1, 0.5),
    ]
    assert iv[0]["start_at"] == "2026-09-24T02:00:00.000000Z"
    assert iv[0]["end_at"] == "2026-09-24T02:04:00.000000Z"
    assert iv[0]["extreme_at"] == "2026-09-24T02:04:00.000000Z"
    # 区间按起始时间升序
    starts = [i["start_at"] for i in iv]
    assert starts == sorted(starts)


def test_boundary_values_are_normal_and_exact_gap_joins(client):
    _sample(client)
    _profile(client, lo=2.0, hi=8.0, gap=60)
    for t, v in [
        ("2026-09-24T10:00:00+08:00", 1.0),
        ("2026-09-24T10:01:00+08:00", 0.5),   # 恰好 60s -> 仍归同一区间
        ("2026-09-24T10:02:00+08:00", 2.0),   # 等于下限 -> 正常,截断
        ("2026-09-24T10:03:00+08:00", 8.0),   # 等于上限 -> 正常
        ("2026-09-24T10:04:00+08:00", 9.0),
        ("2026-09-24T10:05:01+08:00", 9.1),   # 61s -> 超间隔,另起区间
    ]:
        assert _reading(client, t, v).status_code == 200
    got = [(i["direction"], i["reading_count"], i["extreme_value"])
           for i in _excursions(client).json()["intervals"]]
    assert got == [("low", 2, 0.5), ("high", 1, 9.0), ("high", 1, 9.1)]


def test_tied_extreme_picks_earliest(client):
    _sample(client)
    _profile(client, lo=2.0, hi=8.0, gap=300)
    _reading(client, "2026-09-24T11:00:00+08:00", 9.0)
    _reading(client, "2026-09-24T11:01:00+08:00", 9.0)
    iv = _excursions(client).json()["intervals"]
    assert len(iv) == 1 and iv[0]["extreme_value"] == 9.0
    assert iv[0]["extreme_at"] == "2026-09-24T03:00:00.000000Z"


def test_no_excursions_when_all_normal(client):
    _sample(client)
    _profile(client)
    for m in range(0, 15, 2):
        _reading(client, f"2026-09-24T10:{m:02d}:00+08:00", 5.0)
    body = _excursions(client).json()
    assert body["total_readings"] == 8 and body["intervals"] == []


def test_excursions_without_profile_or_sample_is_404(client):
    _sample(client, "S2")
    r = _excursions(client, "S2")
    assert r.status_code == 404
    assert r.json()["error"] == "temperature_profile_not_found"
    assert _excursions(client, "GHOST").status_code == 404


# ---------------------------------------------------------------- 乱序补录重算

def test_late_backfill_merges_existing_intervals(client):
    """先写入间隔过大的两段 high,再补录中间读数桥接,查询重算后合并为一段。"""
    _sample(client)
    _profile(client, lo=2.0, hi=8.0, gap=300)
    _reading(client, "2026-09-24T10:00:00+08:00", 9.0)
    _reading(client, "2026-09-24T10:10:00+08:00", 9.0)  # 600s,先为两段
    assert [(i["direction"], i["reading_count"])
            for i in _excursions(client).json()["intervals"]] == [
        ("high", 1), ("high", 1)]
    # 补录 10:05(180s+180s 均不超间隔)-> 合并
    assert _reading(client, "2026-09-24T10:05:00+08:00", 9.2).status_code == 200
    iv = _excursions(client).json()["intervals"]
    assert [(i["direction"], i["reading_count"], i["extreme_value"])
            for i in iv] == [("high", 3, 9.2)]
    assert iv[0]["start_at"] == "2026-09-24T02:00:00.000000Z"
    assert iv[0]["end_at"] == "2026-09-24T02:10:00.000000Z"


def test_late_normal_backfill_splits_existing_interval(client):
    """先有一段连续 high,补录区间内的正常读数后,既有区间被拆成两段。"""
    _sample(client)
    _profile(client, lo=2.0, hi=8.0, gap=300)
    for t, v in [
        ("2026-09-24T10:00:00+08:00", 9.0),
        ("2026-09-24T10:04:00+08:00", 9.5),
        ("2026-09-24T10:08:00+08:00", 9.0),
    ]:
        _reading(client, t, v)
    assert len(_excursions(client).json()["intervals"]) == 1
    assert _reading(client, "2026-09-24T10:02:00+08:00", 5.0).status_code == 200
    got = [(i["direction"], i["reading_count"], i["extreme_value"])
           for i in _excursions(client).json()["intervals"]]
    # 10:00 单独一段;10:04/10:08 一段(240s)
    assert got == [("high", 1, 9.0), ("high", 2, 9.5)]


# ---------------------------------------------------------------- 审计不可变

def test_temperature_tables_are_immutable(client):
    _sample(client); _profile(client)
    _reading(client, "2026-09-24T10:00:00+08:00", 5.0)
    conn = next(app.dependency_overrides[get_conn]())
    for sql in (
        "UPDATE temperature_profiles SET min_temp = 0",
        "DELETE FROM temperature_profiles",
        "UPDATE temperature_readings SET value = 0",
        "DELETE FROM temperature_readings",
    ):
        with pytest.raises(sqlite3.Error):
            conn.execute(sql)


# ---------------------------------------------------------------- 并发

def test_concurrent_same_instant_writes_serialize(tmp_path):
    """两笔并发写同一归一化 UTC 时刻:BEGIN IMMEDIATE 串行化,
    同值一笔重放成功、异值一笔冲突;读数只有一条。"""
    db_path = str(tmp_path / "t.db")
    setup = connect(db_path)
    init_db(setup)
    setup.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, version, is_root) VALUES ('S1', 'M', 'mL', 1, 1, 0, 1)"
    )
    setup.execute(
        "INSERT INTO temperature_profiles (sample_id, min_temp, max_temp,"
        " max_interval_seconds) VALUES ('S1', 2, 8, 300)"
    )
    setup.commit()
    setup.close()

    from app.schemas import TemperatureReadingIn

    outcomes: list[tuple[float, object]] = []
    barrier = threading.Barrier(2)

    def worker(value: float) -> None:
        c = connect(db_path)
        barrier.wait()
        try:
            res = tp.add_reading(c, "S1", TemperatureReadingIn(
                measured_at="2026-09-24T10:00:00+08:00", value=value,
            ))
            outcomes.append((value, f"ok:{res.replayed}"))
        except tp.TemperatureError as exc:
            outcomes.append((value, exc.conflicts[0].code))
        finally:
            c.close()

    t1 = threading.Thread(target=worker, args=(9.0,))
    t2 = threading.Thread(target=worker, args=(9.5,))
    t1.start(); t2.start(); t1.join(); t2.join()

    codes = {o[1] for o in outcomes}
    assert codes == {"ok:False", "temperature_reading_conflict"}, outcomes
    check = connect(db_path)
    n = check.execute(
        "SELECT COUNT(*) AS c FROM temperature_readings"
        " WHERE sample_id = 'S1'"
    ).fetchone()["c"]
    assert n == 1
    check.close()


# ---------------------------------------------------------------- 旧库迁移

def test_init_db_creates_temperature_tables_on_legacy_database(tmp_path):
    db_path = str(tmp_path / "legacy.db")
    raw = sqlite3.connect(db_path)
    raw.execute(
        "CREATE TABLE samples (sample_id TEXT PRIMARY KEY, material TEXT NOT NULL,"
        " unit TEXT NOT NULL, initial_amount REAL NOT NULL, remaining REAL NOT NULL,"
        " location TEXT, is_root INTEGER NOT NULL DEFAULT 0, created_at TEXT)"
    )
    raw.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, location, is_root) VALUES"
        " ('S1', 'M', 'mL', 1, 1, NULL, 1)"
    )
    raw.commit()
    raw.close()

    conn = connect(db_path)
    init_db(conn)
    init_db(conn)
    tables = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert {"temperature_profiles", "temperature_readings"} <= tables

    from app.schemas import TemperatureProfileRequest, TemperatureReadingIn

    p = tp.register_profile(conn, "S1", TemperatureProfileRequest(
        min_temp=2, max_temp=8, max_interval_seconds=300))
    assert p.replayed is False
    r = tp.add_reading(conn, "S1", TemperatureReadingIn(
        measured_at="2026-09-24T10:00:00+08:00", value=9.0))
    assert r.measured_at_utc == "2026-09-24T02:00:00.000000Z"
    ex = tp.compute_excursions(conn, "S1")
    assert len(ex.intervals) == 1 and ex.intervals[0].direction == "high"
    conn.close()
