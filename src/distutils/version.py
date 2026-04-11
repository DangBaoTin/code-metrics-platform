"""Compatibility shim for distutils.version on Python 3.12+."""

try:
    from setuptools._distutils.version import LooseVersion  # type: ignore
except Exception:
    class LooseVersion:  # pragma: no cover
        def __init__(self, vstring=None):
            self.vstring = "" if vstring is None else str(vstring)

        def __repr__(self):
            return f"LooseVersion ('{self.vstring}')"

        def __str__(self):
            return self.vstring

        def _parts(self):
            import re

            tokens = re.findall(r"\d+|[A-Za-z]+", self.vstring)
            out = []
            for token in tokens:
                if token.isdigit():
                    out.append((0, int(token)))
                else:
                    out.append((1, token.lower()))
            return out

        def _cmp(self, other):
            if not isinstance(other, LooseVersion):
                other = LooseVersion(str(other))
            a = self._parts()
            b = other._parts()
            if a < b:
                return -1
            if a > b:
                return 1
            return 0

        def __lt__(self, other):
            return self._cmp(other) < 0

        def __le__(self, other):
            return self._cmp(other) <= 0

        def __eq__(self, other):
            return self._cmp(other) == 0

        def __ne__(self, other):
            return self._cmp(other) != 0

        def __gt__(self, other):
            return self._cmp(other) > 0

        def __ge__(self, other):
            return self._cmp(other) >= 0
