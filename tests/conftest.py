"""
pytest configuration for the CLI test suite.

ansible_runner imports pkg_resources at the top level, which is a
setuptools utility not present in the uv-managed test environment. We stub it
out here, before any test module imports modules.coactd, so the module loads
cleanly without requiring the full ansible/setuptools stack at test time.
"""
import sys
from unittest.mock import MagicMock

if "pkg_resources" not in sys.modules:
    sys.modules["pkg_resources"] = MagicMock()
if "ansible_runner" not in sys.modules:
    sys.modules["ansible_runner"] = MagicMock()
