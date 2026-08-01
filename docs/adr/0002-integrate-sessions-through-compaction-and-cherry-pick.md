# Integrate Sessions through Compaction and Cherry-pick

Cross-Session Integration compacts a selected source Branch and cherry-picks the resulting Compaction Commit onto the target as an Agent Message Commit, then optionally settles the source Session. We chose this over multi-parent merging or transcript concatenation to preserve single-parent context, make the handoff deliberate, and keep the source history separately inspectable through provenance.
