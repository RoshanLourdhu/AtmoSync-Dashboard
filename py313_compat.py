import selectors
_cls = getattr(selectors, "_BaseSelectorImpl", selectors.SelectSelector)
_orig = _cls.unregister
def _safe(self, f):
    try:
        return _orig(self, f)
    except ValueError:
        for k in list(self.get_map().values()):
            if k.fileobj is f:
                return _orig(self, k.fd)
        return None
if not getattr(_orig, "_patched", False):
    setattr(_safe, "_patched", True)
    _cls.unregister = _safe
