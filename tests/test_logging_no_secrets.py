"""Tracebacks must not print local variable values (loguru's diagnose mode),
because the password from COACT_PASSWORD is a local in every GraphQL command."""

import re
from unittest.mock import patch

from loguru import logger

from modules.base import configure_logging_from_verbose

SECRET = "hunter2-do-not-log"
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def stderr_text(capsys) -> str:
    return ANSI.sub("", capsys.readouterr().err)


def fail_with_secret_local(password):
    raise RuntimeError("boom")


def test_configure_logging_from_verbose_hides_locals(capsys):
    configure_logging_from_verbose(3)
    try:
        fail_with_secret_local(SECRET)
    except RuntimeError:
        logger.exception("failed")
    err = stderr_text(capsys)
    assert "RuntimeError: boom" in err
    assert SECRET not in err


def test_overage_crash_does_not_log_the_password(capsys):
    import sdf_click

    with patch("modules.coact.FacilityUsage.get", side_effect=RuntimeError("boom")), \
            patch("sys.argv", ["sdf_click.py", "coact", "overage", "--dry-run", "--verbose"]), \
            patch.dict("os.environ", {"COACT_PASSWORD": SECRET}):
        assert sdf_click.main() == 1
    err = stderr_text(capsys)
    assert "RuntimeError: boom" in err
    assert SECRET not in err
