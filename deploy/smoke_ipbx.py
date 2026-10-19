"""Real Portal IPBX export smoke test without retaining customer calls."""
from datetime import datetime, timedelta
from pathlib import Path
import sys
import tempfile

core = Path(__file__).resolve().parents[1] / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"
sys.path.insert(0, str(core))
from toi_ipbx_blocked_calls_v1 import fetch_blocked_calls, build_blocked_calls_report

end = datetime.now() - timedelta(minutes=2)
start = end - timedelta(minutes=1)
data, filename = fetch_blocked_calls("0247400046", start, end, "smoke-linux")
assert data and filename.endswith(".xls")
with tempfile.TemporaryDirectory(prefix="ipbx-smoke-", dir=core.parents[0] / "relatorios") as out:
    report = build_blocked_calls_report("0247400046", start_at=start, end_at=end,
                                        source=data, output_dir=out, job_id="smoke-linux")
    assert Path(report["output"]).is_file()
    print(f"IPBX Portal export, validation and workbook OK; rows={report['rows']}")
