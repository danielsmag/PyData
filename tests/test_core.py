import pytest

from template.core import hello


class TestHello:
    """Test suite for the hello function"""

    def test_hello_basic(self, capsys):
        """Test basic functionality with a simple name"""
        hello("Alice")
        captured = capsys.readouterr()
        assert captured.out == "Hello Alice\n"

    def test_hello_empty_string(self, capsys):
        """Test behavior with empty string"""
        hello("")
        captured = capsys.readouterr()
        assert captured.out == "Hello \n"

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("Bob", "Hello Bob\n"),
            ("Alice Smith", "Hello Alice Smith\n"),
        ],
    )
    def test_hello_various_inputs(self, capsys, name, expected):
        """Test hello function with various input types"""
        hello(name)
        captured = capsys.readouterr()
        assert captured.out == expected

    def test_hello_type_error(self):
        """Test that passing non-string types raises TypeError"""
        with pytest.raises(TypeError):
            hello(123)
