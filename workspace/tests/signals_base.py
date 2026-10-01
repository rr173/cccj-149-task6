"""信号子系统测试公共夹具。"""
import shutil
import tempfile
import unittest
from datetime import datetime

from pv_service.signals.service import SignalApplication, FixedClock

CATALOG = {"concepts": [
    {"code": "IBU", "kind": "DRUG", "name": "布洛芬",
     "terms": ["ibuprofen", "布洛芬", "ibuprofeno"]},
    {"code": "AMX", "kind": "DRUG", "name": "阿莫西林",
     "terms": ["amoxicillin", "阿莫西林"]},
    {"code": "WAR", "kind": "DRUG", "name": "华法林",
     "terms": ["warfarin", "华法林"]},
    {"code": "RASH", "kind": "EVENT", "name": "皮疹",
     "terms": ["rash", "皮疹", "sarpullido"]},
    {"code": "NAU", "kind": "EVENT", "name": "恶心",
     "terms": ["nausea", "恶心"]},
    {"code": "BLD", "kind": "EVENT", "name": "出血",
     "terms": ["bleeding", "出血"]},
]}

DEFAULT_SPEC = {
    "drug_groups": [
        {"id": "D_IBU", "name": "布洛芬组", "drug_codes": ["IBU"]},
        {"id": "D_AMX", "name": "阿莫西林组", "drug_codes": ["AMX"]},
        {"id": "D_WAR", "name": "华法林组", "drug_codes": ["WAR"]},
    ],
    "event_groups": [
        {"id": "E_RASH", "name": "皮肤事件", "concept_codes": ["RASH"]},
        {"id": "E_GI", "name": "胃肠事件", "concept_codes": ["NAU"]},
        {"id": "E_BLD", "name": "出血事件", "concept_codes": ["BLD"]},
    ],
    "thresholds": {"min_cases": 2, "min_prr": 2.0, "min_ror": 2.0,
                   "min_chi_square": 3.841},
}


class BaseSignalTest(unittest.TestCase):
    START = datetime(2026, 10, 1, 9, 0, 0)
    CATALOG_ID = "terms@1"
    DEFINITION_ID = "MON1"
    W1 = ("2026-09-01", "2026-10-01", "2026-10-02T00:00:00")
    W2 = ("2026-09-01", "2026-10-01", "2026-11-01T00:00:00")

    def setUp(self, shard_size: int = 50, crash_hook=None):
        self.dir = tempfile.mkdtemp(prefix="sig-test-")
        self.clock = FixedClock(self.START)
        self.app = SignalApplication(
            self.dir, clock=self.clock, shard_size=shard_size,
            crash_hook=crash_hook)
        self.app.catalogs.register(self.CATALOG_ID, CATALOG)
        self.app.definitions.create(
            self.DEFINITION_ID, DEFAULT_SPEC,
            term_catalog_id=self.CATALOG_ID)
        self._window2_armed = False

    def _arm_window2(self):
        """推进时钟，使后续写入落在 W1 截止线之后（进入 W2 的版本可见集）。"""
        if not self._window2_armed:
            self.clock.set(datetime(2026, 10, 10, 9, 0, 0))
            self._window2_armed = True

    def tearDown(self):
        self.app.shutdown()
        shutil.rmtree(self.dir, ignore_errors=True)

    # -- 构造器 -------------------------------------------------------------
    def source(self, sid, drugs, events, day, *, origin="HOSPITAL",
               causality=None, event_causality=None):
        payload = self.payload(drugs, events, causality=causality,
                               event_causality=event_causality)
        self.app.sources.intake(sid, origin, payload, event_date=day)
        return payload
    @staticmethod
    def payload(drugs, events, *, causality=None, event_causality=None):
        ds = []
        for d in drugs:
            if isinstance(d, tuple):
                ds.append({"name": d[0], "causality": d[1]})
            else:
                item = {"name": d}
                if causality:
                    item["causality"] = causality
                ds.append(item)
        es = []
        for e in events:
            item = {"term": e}
            if event_causality:
                item["causality"] = event_causality
            es.append(item)
        return {"drugs": ds, "events": es}

    def followup(self, sid, drugs, events, **kw):
        return self.app.sources.add_version(
            sid, self.payload(drugs, events), **kw)

    def submit_full(self, window=W1, *, mode="FULL", wait=True,
                    term_catalog_id=None, definition_version_no=None,
                    shard_size=None):
        job = self.app.runner.submit(
            definition_id=self.DEFINITION_ID,
            window_start=window[0], window_end=window[1],
            cutoff=window[2],
            term_catalog_id=term_catalog_id or self.CATALOG_ID,
            mode=mode, definition_version_no=definition_version_no,
            shard_size=shard_size)
        if wait and job["status"] != "COMPLETED":
            self.app.runner.run_to_completion(job["id"])
            job = self.app.runner.get_job(job["id"])
        return job

    def run_of(self, job):
        return self.app.results.get_run(job["run_id"])

    def pair(self, run, dg, eg):
        return next(p for p in run["pairs"]
                    if p["drug_group_id"] == dg and p["event_group_id"] == eg)
