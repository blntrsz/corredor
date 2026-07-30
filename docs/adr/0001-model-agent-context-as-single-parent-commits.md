# Model agent context as single-parent Commits

Corredor represents agent context as immutable Commits with at most one parent, runs Agents statelessly from a chosen Commit, and keeps each Peer's Branch Head local. This favors reproducible, portable Agent Runs and conflict-free divergence at the cost of reconstructing context for every run and integrating work without multi-parent ancestry.
