"""Terraform validation helpers.

The `terraform` binary may not be available in every environment (it requires
system packages). This module provides two layers:

  1. If `terraform` is on PATH: run `terraform fmt -check` and `terraform
     validate` and report real results.
  2. Otherwise: parse every `.tf` file with HCL2 to catch syntax errors and
     check for the required structural elements (module blocks, provider,
     variables). This is a weaker but honest substitute, and the final report
     clearly states which path was used.

CI uses the real `terraform` binary (see .github/workflows/ci.yml).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any


def find_terraform() -> str | None:
    return shutil.which("terraform")


def run_terraform_validate(root: str = "infra/terraform") -> dict[str, Any]:
    tf = find_terraform()
    if tf is None:
        return {"available": False, "tool": None}
    fmt = subprocess.run([tf, "fmt", "-check", "-recursive", root], capture_output=True, text=True)
    init = subprocess.run(
        [tf, "-chdir=" + root, "init", "-backend=false", "-input=false"],
        capture_output=True,
        text=True,
    )
    validate = subprocess.run(
        [tf, "-chdir=" + root, "validate", "-no-color"], capture_output=True, text=True
    )
    return {
        "available": True,
        "tool": tf,
        "fmt_ok": fmt.returncode == 0,
        "fmt_output": (fmt.stdout + fmt.stderr).strip(),
        "init_ok": init.returncode == 0,
        "init_output": (init.stdout + init.stderr).strip()[-2000:],
        "validate_ok": validate.returncode == 0,
        "validate_output": (validate.stdout + validate.stderr).strip(),
    }


def parse_hcl(root: str = "infra/terraform") -> dict[str, Any]:
    """Parse all .tf files as HCL2; return syntax status and structure counts."""
    try:
        import hcl2
    except ImportError:
        return {"hcl_parser": False, "detail": "python-hcl2 not installed"}

    root_path = Path(root)
    files = sorted(root_path.rglob("*.tf"))
    errors: list[dict[str, str]] = []
    parsed: dict[str, Any] = {}
    for f in files:
        try:
            with open(f, encoding="utf-8") as fh:
                parsed[str(f.relative_to(root_path))] = hcl2.load(fh)
        except Exception as exc:
            errors.append({"file": str(f.relative_to(root_path)), "error": str(exc)})

    module_count = sum(len(v.get("module", [])) for v in parsed.values())
    resource_count = sum(len(v.get("resource", [])) for v in parsed.values())
    variable_count = sum(len(v.get("variable", [])) for v in parsed.values())
    provider_count = sum(len(v.get("provider", [])) for v in parsed.values())
    return {
        "hcl_parser": True,
        "files": len(files),
        "syntax_errors": errors,
        "syntax_ok": not errors,
        "modules": module_count,
        "resources": resource_count,
        "variables": variable_count,
        "providers": provider_count,
    }


def validate_all(root: str = "infra/terraform") -> dict[str, Any]:
    report: dict[str, Any] = {
        "terraform_binary": run_terraform_validate(root),
        "hcl_parse": parse_hcl(root),
    }
    if report["terraform_binary"]["available"]:
        report["validated_with"] = "terraform binary"
        report["ok"] = bool(report["terraform_binary"]["validate_ok"])
    elif report["hcl_parse"].get("hcl_parser"):
        report["validated_with"] = "hcl2 parser (terraform binary unavailable)"
        report["ok"] = bool(report["hcl_parse"]["syntax_ok"])
    else:
        # Neither the terraform binary nor the HCL2 parser is available: report
        # honestly that validation could not run instead of crashing.
        report["validated_with"] = "none (terraform binary and python-hcl2 unavailable)"
        report["ok"] = False
    return report


if __name__ == "__main__":
    print(json.dumps(validate_all(), indent=2))
