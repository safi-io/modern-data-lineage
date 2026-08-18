# Update DATA_PLATFORM_CONTEXT.md

Re-analyze the current repository and update the existing:

`DATA_PLATFORM_CONTEXT.md`

The purpose of this task is to keep `DATA_PLATFORM_CONTEXT.md` synchronized with the **current state of the repository** after new features, components, pipelines, configurations, or architectural changes have been added.

This is a **context/documentation synchronization task only**.

I will use `DATA_PLATFORM_CONTEXT.md` as context for other LLMs when discussing this data platform.

---

## 1. Inspect the current repository

First, inspect the repository again.

Do not assume that the existing `DATA_PLATFORM_CONTEXT.md` is completely accurate.

Compare the current repository against the existing context document.

Look for:

* New files
* Deleted files
* Modified files
* New services
* New pipelines
* New Spark jobs
* New Dagster assets/jobs/sensors/schedules
* New dbt models
* New Kafka topics/producers/consumers
* New CDC components
* New Iceberg tables
* New storage paths
* New data sources
* New data sinks
* New infrastructure
* New Docker services
* New AWS resources
* New configuration
* New environment variables
* New observability components
* Changes to existing components
* Changes to data flows
* Changes to deployment architecture

---

## 2. Update, don't blindly regenerate

Use the existing `DATA_PLATFORM_CONTEXT.md` as the starting point.

Preserve useful existing information that is still accurate.

Update information that has changed.

Add information for newly implemented functionality.

Remove information that is no longer true.

Correct outdated information.

Do not simply append new information to the end of the document.

Maintain a coherent representation of the **current architecture**.

---

## 3. Repository is the source of truth

The current repository implementation is the primary source of truth.

If the existing context document says something different from the current code/configuration:

**Update the context document to reflect the repository.**

Do not preserve outdated information merely because it exists in the previous context document.

---

## 4. Maintain the same purpose

`DATA_PLATFORM_CONTEXT.md` should remain a factual description of the current platform.

It should explain:

* What exists
* How components work together
* How data flows
* How the platform is configured
* How the platform is deployed
* How data is stored
* How data is processed
* How data is transformed
* How orchestration works
* How metadata works
* How observability works
* What is currently implemented

---

## 5. Do NOT add analysis

Do not add:

* Recommendations
* Improvements
* Optimizations
* Technical debt
* Architectural criticism
* Bottleneck analysis
* Security recommendations
* Scalability recommendations
* Alternative architectures
* Future architecture proposals

I will handle those myself.

Your job is to maintain **context**, not provide architectural advice.

---

## 6. Preserve implementation status

Maintain the distinction between:

* Implemented
* Partially implemented
* Configured but unused
* Mentioned/documented but not implemented
* Unknown

If a previously unimplemented feature has now been implemented, update its status.

If a previously implemented feature has been removed, update its status accordingly.

---

## 7. Update data flows

If new components or pipelines have been introduced, update the relevant end-to-end data flows.

For example, if the repository changes from:

```text
Source
  ↓
Ingestion
  ↓
Bronze
  ↓
Spark
  ↓
Silver
  ↓
dbt
  ↓
Gold
```

to:

```text
Source
  ↓
CDC
  ↓
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze
  ↓
Silver
  ↓
dbt
  ↓
Gold
```

then update the architecture and all relevant sections of `DATA_PLATFORM_CONTEXT.md`.

Do not leave the old architecture alongside the new architecture unless both actually exist.

---

## 8. Update the architecture diagram

Update the Mermaid architecture diagram to represent the **current repository**.

The diagram must reflect:

* New components
* Removed components
* Changed relationships
* New data flows
* Changed processing paths

Do not leave outdated components in the diagram.

---

## 9. Update the technology stack

Update the technology stack whenever technologies are:

* Added
* Removed
* Replaced
* Repurposed

Keep descriptions repository-specific.

Do not add generic descriptions of technologies.

---

## 10. Update repository structure

If new important directories or components have been added, update the repository map.

Remove directories that no longer exist.

Do not document every minor file.

---

## 11. Update configuration

If configuration has changed, update the configuration section.

Include:

* New important environment variables
* New Spark configuration
* New Kafka configuration
* New Dagster configuration
* New dbt configuration
* New Iceberg configuration
* New infrastructure configuration

Never include secret values.

Only document the variable/configuration name and its purpose.

---

## 12. Update LLM QUICK CONTEXT

The:

`# LLM QUICK CONTEXT`

section is especially important.

Rewrite it when necessary so that it remains an accurate, compact representation of the current platform.

It should allow another LLM to quickly understand:

* What the platform is
* Current architecture
* Technology stack
* Current data flow
* Storage
* Processing
* Orchestration
* Transformation
* Streaming/CDC
* Data modeling
* Infrastructure
* Current implementation state

Do not let this section become outdated after adding features.

---

## 13. Check for stale information

Before finishing, explicitly compare the existing context against the repository and look for:

* Components that no longer exist
* Old architecture diagrams
* Old data flows
* Old technology versions
* Old directory names
* Old service names
* Removed pipelines
* Incorrect implementation statuses
* Configuration that no longer exists
* Features described as planned that are now implemented
* Features described as implemented that have been removed

Correct these in `DATA_PLATFORM_CONTEXT.md`.

---

## 14. Accuracy rules

Follow these rules:

1. Repository code/configuration is the source of truth.
2. Do not hallucinate functionality.
3. Do not infer functionality merely from dependencies.
4. Do not copy secrets.
5. Clearly distinguish implemented vs planned functionality.
6. Keep descriptions specific to this repository.
7. Preserve accurate existing context.
8. Remove outdated context.
9. Update relationships between components when architecture changes.
10. Keep the document useful as context for another LLM.

---

## 15. Final verification

Before finishing, verify that `DATA_PLATFORM_CONTEXT.md` answers:

* What is the platform?
* What technologies does it currently use?
* What are the current components?
* How do they communicate?
* Where does data come from?
* How is data ingested?
* Where is data stored?
* How is data processed?
* How is data transformed?
* How is the platform orchestrated?
* How does streaming/CDC work, if implemented?
* How is metadata handled?
* How is observability implemented?
* How is the platform deployed?
* What is actually implemented right now?

If the answer to any of these has changed because of recent repository changes, update the context.

---

## Final constraint

Modify **only**:

`DATA_PLATFORM_CONTEXT.md`

Do not modify application code, infrastructure, configuration, tests, or any other repository files.
