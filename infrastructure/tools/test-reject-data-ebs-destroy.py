#!/usr/bin/env python3
import json, subprocess, sys, tempfile
from pathlib import Path
guard = Path(__file__).with_name("reject-data-ebs-destroy.py")
def check(payload, allowed):
    with tempfile.NamedTemporaryFile("w", suffix=".json") as stream:
        json.dump(payload, stream); stream.flush()
        result=subprocess.run([sys.executable, str(guard), stream.name], capture_output=True, text=True)
    assert (result.returncode == 0) is allowed, (payload, result.stdout, result.stderr)
base=lambda actions,address="aws_ebs_volume.data": {"resource_changes":[{"address":address,"type":"aws_ebs_volume","change":{"actions":actions}}]}
check(base(["create"]), True)
check(base(["update"]), True)
check(base(["delete"]), False)
check(base(["delete","create"]), False)
check(base(["create","delete"]), False)
check(base(["delete"], "module.host.aws_ebs_volume.data"), False)
check({}, False)
check({"resource_changes":[{"address":"aws_ebs_volume.data","type":"aws_ebs_volume","change":{}}]}, False)
print("EBS plan guard fixtures: PASS")
