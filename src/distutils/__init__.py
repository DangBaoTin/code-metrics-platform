"""Compatibility shim for Python 3.12 where distutils is removed."""

from .version import LooseVersion

__all__ = ["LooseVersion"]
