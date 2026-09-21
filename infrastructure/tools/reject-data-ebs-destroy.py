#!/usr/bin/env python3
import json, sys
if len(sys.argv) != 2:
    raise SystemExit("usage: reject-data-ebs-destroy.py PLAN.json")
try:
    with open(sys.argv[1], encoding="utf-8") as stream:
        plan = json.load(stream)
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"invalid plan JSON: {error}")
changes = plan.get("resource_changes")
if not isinstance(changes, list):
    raise SystemExit("invalid plan JSON: resource_changes array is required")
bad=[]
for item in changes:
    if not isinstance(item, dict) or not isinstance(item.get("change"), dict):
        raise SystemExit("invalid plan JSON: malformed resource change")
    resource_type = item.get("type")
    address = item.get("address", "")
    if resource_type is None and address.split(".")[-2:-1] == ["aws_ebs_volume"]:
        resource_type = "aws_ebs_volume"
    actions = item["change"].get("actions")
    if not isinstance(actions, list):
        raise SystemExit("invalid plan JSON: change actions array is required")
    if resource_type == "aws_ebs_volume" and "delete" in actions:
        bad.append(address or "<unknown aws_ebs_volume>")
if bad:
    raise SystemExit("refusing plan that deletes/replaces EBS: " + ", ".join(bad))
print("EBS plan guard: PASS")
