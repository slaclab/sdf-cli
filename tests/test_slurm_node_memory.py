"""
Unit tests for SLURM node memory adjustment functionality.

Tests the parsing of SLURM compressed node lists and memory adjustment
for high-memory nodes (sdfmilan[269-272]).
"""

from modules.coact import SlurmImporter


class TestSlurmNodelistParsing:
    """Test SLURM nodelist parsing functionality."""

    def setup_method(self):
        """Create a SlurmImporter instance for testing."""
        self.importer = SlurmImporter(
            username="test",
            password_file="test",
            verbose=False,
            exit_on_error=False
        )

    def test_parse_slurm_nodelist_single(self):
        """Test parsing a single node name."""
        result = self.importer.parse_slurm_nodelist("sdfmilan0271")
        assert result == ["sdfmilan0271"]

    def test_parse_slurm_nodelist_range(self):
        """Test parsing a SLURM node range."""
        result = self.importer.parse_slurm_nodelist("sdfmilan[269-272]")
        expected = ["sdfmilan269", "sdfmilan270", "sdfmilan271", "sdfmilan272"]
        assert result == expected

    def test_parse_slurm_nodelist_list(self):
        """Test parsing a comma-separated list of nodes."""
        result = self.importer.parse_slurm_nodelist("sdfmilan[006,011,027]")
        expected = ["sdfmilan006", "sdfmilan011", "sdfmilan027"]
        assert result == expected

    def test_parse_slurm_nodelist_mixed(self):
        """Test parsing a mixed range and list."""
        result = self.importer.parse_slurm_nodelist("sdfmilan[001-003,010,020-022]")
        expected = [
            "sdfmilan001", "sdfmilan002", "sdfmilan003",
            "sdfmilan010",
            "sdfmilan020", "sdfmilan021", "sdfmilan022"
        ]
        assert result == expected

    def test_parse_slurm_nodelist_different_prefix(self):
        """Test parsing with different node prefix."""
        result = self.importer.parse_slurm_nodelist("sdfrome[001-003]")
        expected = ["sdfrome001", "sdfrome002", "sdfrome003"]
        assert result == expected

    def test_parse_slurm_nodelist_unparseable(self):
        """Test that unparseable format returns original string."""
        result = self.importer.parse_slurm_nodelist("invalid[format")
        assert result == ["invalid[format"]

    def test_parse_slurm_nodelist_empty(self):
        """Test parsing empty string."""
        result = self.importer.parse_slurm_nodelist("")
        assert result == [""]
