# Copyright (c) Microsoft Corporation.

def get_text(self, separator=u"", strip=False,
                types=(NavigableString, CData)):
    """
    Get all child strings, concatenated using the given separator.
    """
    return separator.join([s for s in self._all_strings(
                strip, types=types)])

def _all_strings(self, strip=False, types=(NavigableString, CData)):
    """Yield all strings of certain classes, possibly stripping them.

    By default, yields only NavigableString and CData objects. So
    no comments, processing instructions, etc.
    """
    for descendant in self.descendants:
        if (
            (types is None and not isinstance(descendant, NavigableString))
            or
            (types is not None and type(descendant) not in types)):
            continue
        if strip:
            descendant = descendant.strip()
            if len(descendant) == 0:
                continue
        yield descendant