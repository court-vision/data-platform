"""
The news feed's rules (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md § 3): an item's kind, the hash
it is deduplicated on across sources, and the players a free-text item names.

`classify` is the plan's v1 keyword table, read over the headline first and the body only
when the headline says nothing: a Rotowire headline ("Suffers leg fracture") is the item's
point, and a recap's body can mention an old injury. The first rule that matches wins, in
this order: rumor (a trade market is not a trade), transaction, a player leaving a game
hurt, a stat-line recap (Rotowire's "totaled 20 points (6-11 FG, ..." sentence, whose
words would otherwise read as news), availability, injury, lineup, performance words,
else other.
"""

from __future__ import annotations

import hashlib
import re
from typing import Iterable, Mapping, Optional

from pipelines.transformers import normalize_name

_DAYS = r"(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|tonight|tomorrow)"

_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("rumor", (
        r"\btrade (?:talks|market|candidate|block|interest|rumou?rs?|chatter)\b",
        r"\brumou?rs?\b",
        r"\binterest(?:ed)? in (?:acquiring|trading|signing)\b",
        r"\bon the (?:trade )?block\b",
        r"\bcould be (?:traded|moved|dealt)\b",
        r"\bexploring (?:a )?trades?\b",
    )),
    ("transaction", (
        r"\btrad(?:e|ed|es|ing)\b",
        r"\b(?:re-)?sign(?:s|ed|ing)? (?:a|an|with|to|him|his|on)\b",
        r"\bto sign\b",
        r"\bwaive[sd]?\b|\bwaiving\b|\bclaimed off waivers\b",
        r"\breleased?\b|\breleases\b",
        r"\bextension\b|\btwo-way\b|\b(?:10|ten)-day\b|\bbuyout\b",
        r"\b(?:assign(?:ed|s)?|recalled|sent down) (?:to|from) the g league\b",
        r"\bg league\b",
        r"\bacquire[sd]?\b",
        r"\bfree agen(?:t|cy)\b",
        r"\bagree(?:s|d)? to (?:a |an )?(?:[\w$.-]+ )*?(?:contract|deal)\b",
        r"\b(?:exercis|declin|pick)\w*(?: up)? (?:[\w$.',-]+ )*?options?\b",
    )),
    ("injury", (
        r"\b(?:leaves?|left|exits?|exited) (?:the |\w+'s )?(?:game|contest|win|loss)\b",
        r"\b(?:did not|didn't|won't|will not|failed to) return\b",
    )),
    ("performance", (r"\b\d+ points \(\d+-\d+ fg\b",)),
    ("availability", (
        r"\bruled out\b",
        r"\b(?:won't|will not|didn't|did not|isn't expected to|not expected to|unlikely to) (?:play|suit up|return)\b",
        r"\b(?:is|was|remains|will be|has been|been|listed as|considered|upgraded to|downgraded to)\s+"
        r"(?:out|questionable|doubtful|probable|available|inactive|active)\b",
        r"\bgame-time decision\b|\bgtd\b",
        r"\bout (?:for|against|vs\.?|indefinitely|tonight|again)\b",
        r"\b(?:will|to|expected to|set to|plans to|cleared to|able to) (?:play|return|suit up|make (?:his|her) debut)\b",
        r"\bupgraded\b|\bdowngraded\b",
        r"\bsit(?:s|ting)? out\b",
        r"\bmiss(?:es|ed|ing)? (?:" + _DAYS + r"|\w+'s (?:game|contest|matchup|tilt)|"
        r"(?:the|his|a|another) (?:game|contest|matchup|tilt|games|week|weeks|month|season|rest)|"
        r"\d+ (?:games?|weeks?)|time)\b",
        r"\b(?:for|will|to) rest\b|\bload management\b|\bmaintenance\b",
        r"\bavailable (?:for|to play|" + _DAYS + r")\b",
        r"\bin(?:active|available)\b",
        r"\bsuspen(?:ded|sion)\b",
    )),
    ("injury", (
        r"\binjur(?:y|ies|ed)\b",
        r"\bsprain(?:s|ed)?\b|\bstrain(?:s|ed)?\b",
        r"\bsurgery\b|\bmri\b|\bx-rays?\b|\bre-?evaluated\b",
        r"\bsore(?:ness)?\b|\bfracture[sd]?\b|\btorn\b|\btear\b|\bconcussion\b",
        r"\bcontusion\b|\btendin(?:itis|osis)\b|\btendonitis\b|\bbruise[sd]?\b",
        r"\bsetback\b|\brehab(?:bing|ilitation)?\b",
        r"\billness\b|\bsick\b",
    )),
    ("lineup", (
        r"\b(?:will|to|set to|expected to|slated to|get(?:s|ting)? the) start\b",
        r"\bstarting lineup\b|\bstarting (?:five|unit|role)\b",
        r"\bstart(?:s|ing)? (?:in place of|for the injured)\b",
        r"\b(?:moves?|moved|moving|sent|demoted) to the bench\b",
        r"\b(?:bench|reserve|sixth-man) role\b",
        r"\b(?:come|comes|coming) off the bench\b",
        r"\bminutes (?:restriction|limit|cap)\b|\bout of the rotation\b|\brotation\b",
        r"\bfirst unit\b|\bsecond unit\b",
    )),
    ("performance", (
        r"\b(?:scores?|scored|scoring|posts?|posted|records?|recorded|tallies|tallied|notch(?:es|ed)|"
        r"totals?|totaled|erupts?|erupted|drops?|dropped|pours? in|leads?)\b",
        r"\bdouble-double\b|\btriple-double\b|\bcareer-high\b|\bseason-high\b|\bcareer highs?\b",
        r"\b\d+ (?:points|rebounds|assists|boards|dimes|blocks|steals)\b",
        r"\bstruggles?\b|\bquiet (?:night|outing|showing)\b|\bstrong (?:showing|outing|effort)\b",
    )),
)
_COMPILED = tuple((kind, tuple(re.compile(p, re.I) for p in patterns)) for kind, patterns in _RULES)


def _kind_of(text: str) -> Optional[str]:
    for kind, patterns in _COMPILED:
        if any(p.search(text) for p in patterns):
            return kind
    return None


def classify(headline: str, body: Optional[str] = None) -> str:
    """The item's kind: from the headline, else the body, else 'other'."""
    return _kind_of(headline or "") or (_kind_of(body) if body else None) or "other"


def kind_for(source: str, headline: str, body: Optional[str] = None) -> str:
    """`classify` for one source's item. A Rotowire title's "Name:" is left out; a note on ESPN's
    injury list is about a hurt player, so one that reads as a move (a G League rehab
    assignment) or as nothing is an injury item."""
    if source == "rotowire_rss":
        headline = headline.partition(":")[2].strip() or headline
    kind = classify(headline, body)
    if source == "espn_injuries" and kind in ("transaction", "rumor", "other"):
        return "injury"
    return kind


def content_hash(text: str) -> str:
    """sha256 of the text as words: case, accents, punctuation and spacing don't count."""
    words = re.sub(r"[^a-z0-9]+", " ", normalize_name(text or "")).strip()
    return hashlib.sha256(words.encode("utf-8")).hexdigest()


# ---- names in free text ----------------------------------------------------------------

_SUFFIX = re.compile(r"\s+(?:jr\.?|sr\.?|ii|iii|iv|v)$")
_TOKEN = re.compile(r"[a-z0-9][a-z0-9.'\-]*")


class NameScanner:
    """Finds the players a text names, by full name (`by_name`: normalized name -> NBA id).

    A name with a suffix also matches without it ("Jaren Jackson" for "Jaren Jackson Jr."),
    when no other player has the short form. Longest match first; a possessive or a
    sentence's full stop doesn't stop a match.
    """

    MAX_WORDS = 4

    def __init__(self, by_name: Mapping[str, int]):
        short_forms: dict[str, Optional[int]] = {}
        for name, pid in by_name.items():
            short = _SUFFIX.sub("", name)
            if short != name and short.count(" ") >= 1 and short not in by_name:
                short_forms[short] = pid if short_forms.get(short, pid) == pid else None
        self._names = dict(by_name)
        self._names.update({n: pid for n, pid in short_forms.items() if pid is not None})

    def find(self, *texts: Optional[str]) -> list[int]:
        found: list[int] = []
        for text in texts:
            if not text:
                continue
            tokens = _TOKEN.findall(normalize_name(text).replace("’", "'"))
            i = 0
            while i < len(tokens):
                for n in range(min(self.MAX_WORDS, len(tokens) - i), 1, -1):
                    pid = self._lookup(" ".join(tokens[i:i + n]))
                    if pid is not None:
                        if pid not in found:
                            found.append(pid)
                        i += n
                        break
                else:
                    i += 1
        return found

    def _lookup(self, phrase: str) -> Optional[int]:
        for candidate in (phrase, phrase.rstrip("."), re.sub(r"'s?\.?$", "", phrase)):
            if candidate in self._names:
                return self._names[candidate]
        return None


def linked_teams(own: Iterable[str], players: Iterable[int], team_of: Mapping[int, str],
                 known: Optional[set[str]] = None) -> list[str]:
    """The NBA teams an item is about: the ones its source names, and its players' teams."""
    teams = {t.upper() for t in own if t}
    teams |= {team_of[p] for p in players if team_of.get(p)}
    return sorted(t for t in teams if known is None or t in known)
