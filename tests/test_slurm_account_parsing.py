import pytest
from modules.coact import parse_account, SlurmRemapper, SlurmImporter


@pytest.mark.parametrize(
    "account,expected",
    [
        ("lcls:foo", ("lcls", "foo")),
        ("lcls:foo@milano", ("lcls", "foo")),
        ("lcls:foo@milano^preemptable", ("lcls", "foo")),
        ("lcls:foo^preemptable@milano", ("lcls", "foo")),
        ("lcls:default@milano", ("lcls", "default")),
        ("lcls:_regular_@milano", ("lcls", "_regular_")),
        ("lcls:_preemptable_@milano", ("lcls", "_preemptable_")),
    ],
)
def test_parse_account(account, expected):
    assert parse_account(account) == expected


@pytest.mark.parametrize("account", ["root", "", "nocolon"])
def test_parse_account_falls_back(account):
    assert parse_account(account, "shared", "default") == ("shared", "default")


@pytest.mark.parametrize(
    "account_in,qos_in,account_out,qos_out",
    [
        # Legacy formats
        ("lcls:foo", "normal", "lcls:foo", "normal"),
        ("lcls:foo", "preemptable", "lcls:foo", "preemptable"),
        ("lcls:foo@milano", "normal", "lcls:foo", "normal"),
        ("lcls:foo@milano", "preemptable", "lcls:foo", "preemptable"),
        # New hierarchy formats
        ("lcls:foo@milano^preemptable", "normal", "lcls:foo", "preemptable"),
        ("lcls:foo@milano^preemptable", "preemptable", "lcls:foo", "preemptable"),
        ("lcls:default@milano", "normal", "lcls:default", "normal"),
    ],
)
def test_slurm_remapper(account_in, qos_in, account_out, qos_out):
    remapper = SlurmRemapper()
    job_dict = {
        "JobID": "12345",
        "User": "testuser",
        "Account": account_in,
        "Partition": "milano",
        "QOS": qos_in,
    }
    result = remapper.remap_job(job_dict)
    assert result is not None
    assert result["Account"] == account_out
    assert result["QOS"] == qos_out


def test_slurm_importer_convert_dual_hierarchy(monkeypatch):
    from modules.coact import parse_datetime
    importer = SlurmImporter(username="test", password_file="dummy")
    importer._clusters = {"milano": {"cpu": 64, "gpu": 0, "mem": 256 * 1073741824}}

    # Mock allocid lookup
    start_dt = parse_datetime(1767000000)
    end_dt = parse_datetime(1768000000)
    importer._allocid = {("lcls", "foo", "milano"): {(start_dt, end_dt): "alloc_123"}}

    index = {
        "JobID": 0,
        "User": 1,
        "Account": 2,
        "Partition": 3,
        "QOS": 4,
        "Start": 5,
        "End": 6,
        "AllocNodes": 7,
        "NCPUS": 8,
        "AllocTRES": 9,
    }

    # Test 1: New preemptable account format
    parts_new_preempt = ["1001", "user1", "lcls:foo@milano^preemptable", "milano", "normal", "1767225600", "1767229200", "1", "4", "cpu=4,mem=16G"]
    converted = importer.convert(index, parts_new_preempt)
    assert converted is not None
    assert converted["allocationId"] == "alloc_123"
    assert converted["qos"] == "preemptable"

    # Test 2: New normal account format
    parts_new_normal = ["1002", "user1", "lcls:foo@milano", "milano", "normal", "1767225600", "1767229200", "1", "4", "cpu=4,mem=16G"]
    converted = importer.convert(index, parts_new_normal)
    assert converted is not None
    assert converted["allocationId"] == "alloc_123"
    assert converted["qos"] == "normal"

    # Test 3: Legacy preemptable QOS format
    parts_legacy_preempt = ["1003", "user1", "lcls:foo@milano", "milano", "preemptable", "1767225600", "1767229200", "1", "4", "cpu=4,mem=16G"]
    converted = importer.convert(index, parts_legacy_preempt)
    assert converted is not None
    assert converted["allocationId"] == "alloc_123"
    assert converted["qos"] == "preemptable"
