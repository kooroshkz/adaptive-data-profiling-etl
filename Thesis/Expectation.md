# Supervisor Writing Expectations

Derived from handwritten annotations on the Introduction draft and bibliography feedback.

---

## Framing and Audience

- Open with the **professional/industry context**, not a student or academic exercise framing.
  - Wrong: *"In university, students typically download a static CSV file…"*
  - Right: *"In data-driven organisations, data is produced continuously from sensors, APIs, user interactions, and external providers."*
- Write for a reader who is a **practitioner or researcher**, not a student audience.

---

## Claims Must Be Backed by Citations

- Every non-trivial factual claim needs an inline citation.
- The annotation `+ citation` appeared on paragraphs asserting general facts about data pipelines and ETL without a reference.
- Prefer **peer-reviewed conference or journal papers** over blog posts or industry articles where one exists.
- Do not leave a claim unsupported and assume it is "common knowledge."

---

## Concrete Examples Over Abstractions

- Abstract statements must be grounded with a **specific, realistic example**.
  - Wrong: *"flags anything that deviates from that learned pattern."*
  - Right: *"…for example, a temperature reading that passes a valid-range check but sits far outside the recent seasonal norm."*
- Examples should be directly relevant to the thesis domain (sensor data, ETL pipelines).

---

## Academic Register and Precision

- Use **formal academic language** throughout; avoid informal phrasing or hedging.
- Prefer precise verbs and nouns:
  - "inherits the defect" over "inherits the problem"
  - "encounters and addresses" over "finds and fixes"
  - "at the point of ingestion" over "at ingestion"
- Avoid typographic shortcuts for introducing terms (backtick-style ``quotes``); introduce a coined term plainly and italicise or use quotes only on first use if needed.

---

## Voice and Agency

- Prefer **active sentences** over passive constructions. ML papers typically assign responsibility to an actor.
  - Wrong (passive): *"A pipeline was designed and implemented…"*
  - Right (active): *"We designed and implemented a pipeline…"*
- Use **"we"** as the subject when describing methodological choices, implementations, and experiments — even for a single-author thesis. It is the norm in ML writing.
- Passive voice is acceptable for describing results or established facts where the actor is irrelevant (*"Anomalies were injected…"* is fine in a description of a dataset).

---

## Term Precision and Specificity

- **Generic ML terms need qualification.** "AutoML" alone is not specific — name what is being automated (e.g., "automated anomaly-detector selection", "automated model selection over PyOD algorithms").
- **Coined project-internal terms** (e.g., "Auto-NN") must be introduced with a recognised parent term and the project alias in parentheses: *"automated neural architecture search (Auto-NN)"*.
  - Auto-NN in this thesis = Optuna-driven search over neural architectures (AE/VAE) and their hyperparameters (layers, learning rate, epochs, batch size). The recognised parent term is **Neural Architecture Search (NAS)**.
- If a term is not widely known in the field, it must be explained in the Introduction before it is used in the Abstract or later sections.

---

## Section Structure

- Do not place floating paragraphs **before the first subsection** of a section. Content belongs inside a named subsection, not in the gap between the section heading and `\subsection{}`.
- A subsection should be **substantive**. A one-paragraph subsection is a sign that either it needs to be expanded or it should be merged elsewhere.
- When answering research questions in the Conclusions, **ground each answer in specific experimental results** (metrics, numbers, comparisons). "Learning-based profiling worked well" is not a conclusion — "F2 reached 0.93–1.00 on stable columns" is.
- Avoid redundancy between a section's preamble and its first subsection. If the preamble summarises the subsection, remove the preamble.

---

## Sentence and Paragraph Discipline

- Each sentence should carry one clear idea; avoid run-ons that bundle motivation and mechanism.
- End paragraphs with a consequence or forward-pointing statement, not a restatement of the opening.
- Do not compress two distinct ideas into one sentence to save space.

---

## Bibliography

- **Do not use ChatGPT or other LLMs** to generate or manage bibliography entries — they hallucinate non-existing references, which is a serious academic integrity issue.
- Verify every entry against **DBLP** (for CS papers) or the publisher's own page.
- Use the correct BibTeX entry type (`@article`, `@inproceedings`, `@misc`) matching the actual publication venue.
- Author names in `LastName, FirstName` format, consistently across all entries.
- Protect capitalisation of acronyms in titles with braces: `{ETL}`, `{AutoML}`, `{PyOD}`.
- Remove DBLP noise fields: `timestamp`, `biburl`, `bibsource`, `editor`, `series`, `month`, location/date embedded in `booktitle`.
- Prefer the peer-reviewed published version over an arXiv/CoRR preprint when one exists on DBLP.
- When a DOI is present, the `url` field is redundant — omit it.
