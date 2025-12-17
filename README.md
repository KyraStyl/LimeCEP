# LimeCEP

### Paper: Handling out-of-order input arrival in CEP engines on the edge combining optimistic, pessimistic and lazy evaluation. 
[arxiv link](https://arxiv.org/pdf/2507.01461v2)
# LimeCEP

---

## Automated Pattern Extraction

This repository also includes an experimental extension of LimeCEP for online pattern exploration, available in the `semi-automated` branch.

The extension implements an add-on mechanism that explores frequent evolutions of a user-defined CEP pattern during query execution. In particular, it supports:
- **Pattern extensions**, which append an additional event type to the original pattern; and
- **Pattern variations**, which replace the last event type of the original pattern with an alternative one.

