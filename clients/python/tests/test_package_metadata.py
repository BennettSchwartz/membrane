"""Packaging metadata tests for the Python client."""

from importlib import resources


def test_package_declares_inline_types() -> None:
    marker = resources.files("membrane").joinpath("py.typed")

    assert marker.is_file()
