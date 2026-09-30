"""Randomized re-verification scheduling for isolated accounts.

Pure schedule math, kept separate from both the service that isolates an
account and the service that re-verifies it, because both need the same
answer and neither should own it. See
``app/services/account_recheck.py`` for the recovery policy this supports.

Why the jitter is not optional
------------------------------
A single isolation event can quarantine a whole batch at the same instant. A
fixed re-check window would therefore wake the entire batch together, on every
pass, forever. Drawing the delay uniformly per account spreads the load once
and keeps it spread.

Why the backoff is capped
------------------------
``2 ** failures`` alone would push a permanently degraded account out to
years, at which point it is indistinguishable from an account that was deleted
upstream. The cap keeps it in the re-check queue forever, at a cost that stays
small, so a genuinely recovered account is still noticed.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

# Widening is capped so a permanently degraded account is still looked at
# occasionally instead of silently disappearing from re-verification forever.
MAX_BACKOFF_EXPONENT = 5
# A due time beyond this is indistinguishable from "never schedule again".
MAX_DELAY_MINUTES = 7 * 24 * 60


def recheck_delay_minutes(
    *,
    failures: int,
    min_minutes: int,
    max_minutes: int,
    rng: random.Random,
) -> int:
    """Draw the next re-verification delay for one account, in minutes.

    ``failures`` is the number of consecutive re-verification rounds the
    account has already failed, so ``0`` means "first attempt after isolation".
    """

    low = max(1, int(min_minutes))
    high = max(low, int(max_minutes))
    base = rng.randint(low, high)
    exponent = min(max(int(failures), 0), MAX_BACKOFF_EXPONENT)
    return min(base * (2**exponent), MAX_DELAY_MINUTES)


def first_recheck_due_at(
    *,
    now: datetime,
    min_minutes: int,
    max_minutes: int,
) -> datetime:
    """Return the first re-verification time for a newly isolated account.

    The account is isolated at ``now`` and must not be re-probed immediately:
    the upstream route that caused the isolation is usually still settling.
    """

    delay = recheck_delay_minutes(
        failures=0,
        min_minutes=min_minutes,
        max_minutes=max_minutes,
        rng=random.Random(),
    )
    return now + timedelta(minutes=delay)
