# Corredor

Corredor coordinates distributed coding-agent work as durable, Git-like context histories. It lets independent peers branch from, exchange, compact, and integrate agent work without taking ownership of source-code history.

## Collaboration

**Peer**:
A running Corredor participant that stores and exchanges agent work and can execute Agents.
_Avoid_: Agent, node, server

**Workstream**:
A durable grouping of related Sessions that may span Peers and Workflows.
_Avoid_: Project, workspace

**Session**:
A closed graph of related Commits whose parent links never cross into another Session. A Session may contain many Branches.
_Avoid_: Branch, conversation

**Settlement**:
The explicit, reversible act of marking a Session done, hiding it from normal views, and preventing new work until it is reopened.
_Avoid_: Close, archive, delete

## Context history

**Commit**:
An immutable, context-bearing node identified by a stable unique ID and having at most one parent in the same Session.
_Avoid_: Event, entry, Git Commit

**Git Commit**:
A source-control commit owned by Git rather than Corredor.
_Avoid_: Commit when discussing source code

**Branch**:
The ancestry path ending at a chosen Commit. A Branch has no name or identity separate from the ID of its head Commit.
_Avoid_: Session, named branch

**Branch Head**:
The Commit currently checked out by a Peer. It is local state and is not moved by Push or Pull.
_Avoid_: Active leaf, current event

**Checkout**:
The local act of moving a Branch Head to an existing Commit without creating a Commit.
_Avoid_: Navigate

**User Commit**:
A user's durable contribution to agent context, including a request to continue after an Interrupt or Failure.
_Avoid_: Prompt, user event

**Agent Message Commit**:
An Agent's durable textual contribution to context, including content brought in by Cherry-pick.
_Avoid_: Reply, assistant event

**Tool Commit**:
An atomic record of a completed tool interaction, including its input and its result or failure.
_Avoid_: Tool call, tool event

**Compaction Commit**:
An Agent-produced summary of the complete active ancestry through its parent. On its source Branch, it replaces that ancestry when constructing context for later Agent Runs while leaving the original Commits inspectable.
_Avoid_: Summary message

**Interrupt Commit**:
The durable outcome of an intentionally stopped Agent Run, including its reason and any visible partial output but no hidden model state.
_Avoid_: Cancellation, pause

**Failure Commit**:
The durable outcome of an Agent Run that ended unexpectedly, including a safe, visible failure reason.
_Avoid_: Interrupt, error event

## Execution

**Agent**:
A reusable definition of a model, its instructions, and its available tools.
_Avoid_: Peer, Agent Run

**Agent Run**:
One stateless execution of an Agent from a chosen Commit. It may append Tool Commits and concludes with exactly one Agent Message, Compaction, Interrupt, or Failure Commit.
_Avoid_: Agent, Session

**Workflow**:
An SDK-defined orchestration of Agent Runs and domain operations such as creating Sessions, compacting context, cherry-picking work, and settling Sessions.
_Avoid_: Agent, script

**Workflow Run**:
One best-effort execution of a Workflow. Its successful domain operations are durable, but its in-progress control flow is not guaranteed to survive failure.
_Avoid_: Agent Run

## Exchange and integration

**Push**:
The transfer of a selected Branch Head, its missing ancestors, and the required Session and Workstream metadata to another Peer. It never transfers source-code changes or moves the receiving Peer's Branch Head.
_Avoid_: Publish

**Pull**:
The import of a selected Branch Head, its missing ancestors, and the required Session and Workstream metadata from another Peer. Imported divergence forms additional Branches rather than a conflict.
_Avoid_: Checkout, merge

**Cherry-pick**:
The copying of an Agent Message or Compaction Commit onto a chosen Branch Head as a new Agent Message Commit with source provenance. It may operate within or across Sessions and performs no deduplication.
_Avoid_: Copy, merge

**Integration**:
A cross-Session Workflow that compacts a selected source Branch and cherry-picks the resulting Compaction Commit onto a target Branch. The user chooses either to integrate only or to integrate and settle the source Session; Integration does not start another Agent Run.
_Avoid_: Merge, synchronization
