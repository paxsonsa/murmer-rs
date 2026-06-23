---
name: schedule_repeat maintenance actor revisit
description: User wants to revisit whether maintenance ticks should go to a separate actor rather than the writer, to avoid locking up the writer during heavy maintenance work
type: project
---

User wants to revisit the schedule_repeat usage in ContainerSupervisorActor. Specifically: should periodic maintenance ticks (compaction, tombstone purge, retention) go to a dedicated MaintenanceActor rather than the ContainerWriterActor, so that a slow compaction cycle doesn't block writes?

**Why:** Heavy maintenance ops (compaction rewrites the entire page store) could tie up the writer's mailbox for seconds, starving incoming writes.

**How to apply:** When designing ContainerSupervisorActor's maintenance scheduling, flag this as an open design question — don't settle on schedule_repeat → writer without discussing whether a separate maintenance actor is better.
