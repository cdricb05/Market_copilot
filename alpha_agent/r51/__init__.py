"""alpha_agent.r51 - the non-equity promotion offensive (Release 51).

Release 50 made the operational manager asset-agnostic and left every
non-equity sleeve behind exactly ONE blocker: ``NO_APPROVED_OPERATIONAL_
SIGNAL`` - an evidence gate, not a code gap. Release 51 attacks that blocker
without inventing evidence:

* :mod:`alpha_agent.r51.promotion_frontier` - ONE evidence-based ranking of
  every serious non-equity sleeve by its remaining, honestly-stated distance
  to a legitimate operational-promotion decision. It is a PURE calculation:
  every input is injected, nothing is read from disk, nothing is written, and
  the score never replaces the real gates
  (``alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES`` decide, alone).
* one new frozen challenger (``r51_fx_xs_carry_cip``) registered through the
  same frozen R46 door as every prior cohort - see
  ``alpha_agent.r46.challengers.R51_SPECS``.

Research only. Paper only. This package can promote nothing, approve nothing,
order nothing and spend nothing.
"""

RELEASE = "R51"

#: Mirrors the R46 convention: these are declarations the audit and the tests
#: read, and the package's behaviour is bound to them.
PROMOTION_ALLOWED = False
AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False
MAY_MUTATE_PRODUCTION = False
CREATES_ORDERS = False
