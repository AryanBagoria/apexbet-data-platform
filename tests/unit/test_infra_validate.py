"""Tests for the Terraform validation fallback.

The `terraform` binary and the optional `python-hcl2` parser may both be absent
in a clean environment. Validation must degrade to an honest "not validated"
report rather than crashing.
"""

from __future__ import annotations

import pytest

from apexbet import infra_validate


def test_validate_all_without_any_tool_reports_not_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    """No terraform binary and no HCL parser: report honestly, do not raise."""
    monkeypatch.setattr(
        infra_validate,
        "run_terraform_validate",
        lambda root="infra/terraform": {"available": False, "tool": None},
    )
    monkeypatch.setattr(
        infra_validate,
        "parse_hcl",
        lambda root="infra/terraform": {"hcl_parser": False, "detail": "python-hcl2 not installed"},
    )

    report = infra_validate.validate_all()

    assert report["ok"] is False
    assert "unavailable" in report["validated_with"]


def test_validate_all_with_hcl_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    """No terraform binary but a working HCL parser: ok tracks syntax_ok."""
    monkeypatch.setattr(
        infra_validate,
        "run_terraform_validate",
        lambda root="infra/terraform": {"available": False, "tool": None},
    )
    monkeypatch.setattr(
        infra_validate,
        "parse_hcl",
        lambda root="infra/terraform": {
            "hcl_parser": True,
            "syntax_ok": True,
            "files": 12,
            "modules": 7,
            "resources": 24,
        },
    )

    report = infra_validate.validate_all()

    assert report["ok"] is True
    assert "hcl2 parser" in report["validated_with"]


def test_validate_all_with_terraform_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the terraform binary is present, its result is authoritative."""
    monkeypatch.setattr(
        infra_validate,
        "run_terraform_validate",
        lambda root="infra/terraform": {
            "available": True,
            "tool": "/usr/bin/terraform",
            "validate_ok": True,
        },
    )
    monkeypatch.setattr(
        infra_validate,
        "parse_hcl",
        lambda root="infra/terraform": {"hcl_parser": False, "detail": "python-hcl2 not installed"},
    )

    report = infra_validate.validate_all()

    assert report["ok"] is True
    assert report["validated_with"] == "terraform binary"
