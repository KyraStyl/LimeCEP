
## SEMANTICS - EXECUTION 

We explain how match semantics are applied in practice, by demonstrating the differences between selection policies, the identification of valid matches, maximal matches, and match correction under out-of-order arrivals using a concrete input stream and query.

### Pattern and Query Setup

- **Pattern:** `SEQ(A, B+, C)` (i.e., A followed by one or more B's followed by C)  
- **Window:** *Wp* = 10 seconds

Each event is timestamped at one-second intervals from the previous, and the identifier of an event includes both its type and an index (e.g., `a3`, `b8`).

---

### In-Order Event Stream

```
b1 b2 a3 a4 a5 a6 a7 b8 a9 c10 b11 b12 a13 b14 a15 b16 a17 a18 c19 c20
```

In this example, we focus on the output for pattern `A B+ C`, checking the **maximality** of matches (i.e., no more B events can be added). The matches produced per system and selection policy are shown below. SASE, FlinkCEP, and LimeCEP support both policies; SASEXT only supports STAM.

---

### SASE

#### STNM (Matches: 12)

- `[a3, b8, c10]`, `[a4, b8, c10]`, `[a5, b8, c10]`, `[a6, b8, c10]`, `[a7, b8, c10]`
- `[a9, b11, c19]`, `[a9, b11, b12, c19]`, `[a9, b11, b12, b14, c19]`, `[a9, b11, b12, b14, b16, c19]`
- `[a13, b14, c19]`, `[a13, b14, b16, c19]`, `[a15, b16, c19]`

#### STAM (Matches: 28)

Includes all STNM matches, plus:

- `[a9, b12, b14, c19]`, `[a9, b14, b16, c19]`, ...
- Variants using `c20`: `[a13, b14, b16, c20]`, `[a15, b16, c20]`

---

### FlinkCEP

#### STNM (Matches: 12)

Same as SASE STNM:
- 5 matches with `c10`, 7 with `c19`

#### STAM (Matches: 28)

Same match structure as SASE:
- All `B+` combinations between `A` and `C`, using both `c19` and `c20`

---

### SASEXT

#### STAM (Matches: 10)

- `[a3, b8, c10]`, `[a4, b8, c10]`, `[a5, b8, c10]`, `[a6, b8, c10]`, `[a7, b8, c10]`
- `[a9, b11, b12, b14, b16, c19]`, `[a13, b14, b16, c19]`, `[a15, b16, c19]`
- `[a13, b14, b16, c20]`, `[a15, b16, c20]`

---

### LimeCEP

#### STNM (Matches: 8)

- `[a3, b8, c10]`, `[a4, b8, c10]`, `[a5, b8, c10]`, `[a6, b8, c10]`, `[a7, b8, c10]`
- `[a9, b11, b12, b14, b16, c19]`, `[a13, b14, b16, c19]`, `[a15, b16, c19]`

#### STAM (Matches: 10)

Adds:
- `[a13, b14, b16, c20]`, `[a15, b16, c20]`

---

### Notes

- For simple patterns like `SEQ(A, B, C)`, all matches are maximal by default.
- In patterns with Kleene operators, maximality means the Kleene subpattern is fully extended.
- **STNM** selects the earliest valid match per event, disallowing reuse with later events.
- **STAM** allows all valid combinations, leading to higher match counts.

---

## Arrival Order (Out-of-Order)

Event stream with out-of-order arrivals:

```
b1 b2 b11 a3 c10 a4 a6 c20 a5 a18 a7 b8 a17 a9 a13 b14 b16 a15 c19 b12
```

We now walk through how LimeCEP handles inconsistencies and out-of-order arrivals:

---

### Step-by-Step Execution

#### 1. Arrival of `c10`

No matches yet: only `a3` has arrived but no `B` in between.

#### 2. Arrival of `a3`, `a4`, `a6`, `a5`, `a7`

These events are buffered. CEP waits for a slack window (e.g., 2–3 units) before processing. No matches detected yet.

#### 3. Late arrival of `b8`

CEP is triggered using `c10` as the end event. Matches found:

- `[a3, b8, c10]`, `[a4, b8, c10]`, `[a5, b8, c10]`, `[a6, b8, c10]`, `[a7, b8, c10]`  
(*All valid and maximal at this point*)

#### 4. Late arrival of `a15`

CEP is triggered using `c20`. New matches:

- `[a15, b16, c20]`
- `[a13, b14, b16, c20]`

#### 5. Late arrival of `c19`

Triggers CEP again:

- `[a15, b16, c19]`
- `[a13, b14, b16, c19]`
- `[a9, b11, b14, b16, c19]`

If STNM is used, previous matches with `c20` are invalidated; with STAM, all are kept.

#### 6. Late arrival of `b12` → Match Correction

Triggers recomputation. New match:

- `[a9, b11, b12, b14, b16, c19]`

This replaces the previous shorter match and updates the user.

---

### Final Notes

- Matches like `[a4, b8, c10]` are valid/maximal at creation.
- `[a9, b11, b14, b16, c19]` was initially valid but later extended with `b12`.
- LimeCEP performs match correction to ensure up-to-date and complete results.

---

## Conclusion

These examples highlight how event processing systems behave under various selection policies and arrival patterns. The out-of-order example shows LimeCEP’s robustness and real-time correction capabilities, ensuring match **validity**, **maximality**, and **completeness** under realistic streaming conditions.
