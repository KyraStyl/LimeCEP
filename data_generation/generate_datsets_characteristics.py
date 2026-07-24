from __future__ import annotations

import csv
import random
from pathlib import Path
from typing import Sequence

EVENT_TYPES = tuple("ABCDEFGHIJ")


def validate_probabilities(probabilities: Sequence[float]) -> None:
    if len(probabilities) != len(EVENT_TYPES):
        raise ValueError(
            f"Expected {len(EVENT_TYPES)} probabilities, "
            f"received {len(probabilities)}."
        )

    if any(p < 0 for p in probabilities):
        raise ValueError("Probabilities cannot be negative.")

    if abs(sum(probabilities) - 1.0) > 1e-8:
        raise ValueError(
            f"Probabilities must sum to 1.0, not {sum(probabilities)}."
        )


def generate_iid_stream(
    number_of_events: int,
    probabilities: Sequence[float],
    seed: int,
) -> list[str]:
    """Generate independent events from a fixed categorical distribution."""
    validate_probabilities(probabilities)

    rng = random.Random(seed)

    return rng.choices(
        population=EVENT_TYPES,
        weights=probabilities,
        k=number_of_events,
    )


def generate_correlated_abc_stream(
    number_of_events: int,
    pattern_probability: float,
    noise_probabilities: Sequence[float],
    seed: int,
) -> list[str]:
    """
    Insert correlated A-B-C sequences among otherwise independent noise.

    pattern_probability controls how often a complete ABC sequence begins.
    """
    validate_probabilities(noise_probabilities)

    if not 0 <= pattern_probability <= 1:
        raise ValueError("pattern_probability must be between 0 and 1.")

    rng = random.Random(seed)
    events: list[str] = []

    while len(events) < number_of_events:
        remaining = number_of_events - len(events)

        if remaining >= 3 and rng.random() < pattern_probability:
            events.extend(["A", "B", "C"])
        else:
            event = rng.choices(
                EVENT_TYPES,
                weights=noise_probabilities,
                k=1,
            )[0]
            events.append(event)

    return events[:number_of_events]


def generate_bursty_stream(
    number_of_events: int,
    background_probabilities: Sequence[float],
    burst_type: str,
    burst_length: int,
    burst_probability: float,
    seed: int,
) -> list[str]:
    """Generate a stream containing occasional bursts of one event type."""
    validate_probabilities(background_probabilities)

    if burst_type not in EVENT_TYPES:
        raise ValueError(f"Unknown burst type: {burst_type}")

    rng = random.Random(seed)
    events: list[str] = []

    while len(events) < number_of_events:
        if rng.random() < burst_probability:
            events.extend([burst_type] * burst_length)
        else:
            event = rng.choices(
                EVENT_TYPES,
                weights=background_probabilities,
                k=1,
            )[0]
            events.append(event)

    return events[:number_of_events]


def generate_phased_stream(
    number_of_events: int,
    phase_probabilities: Sequence[Sequence[float]],
    seed: int,
) -> list[str]:
    """Generate consecutive phases with different event distributions."""
    if not phase_probabilities:
        raise ValueError("At least one phase is required.")

    for probabilities in phase_probabilities:
        validate_probabilities(probabilities)

    rng = random.Random(seed)
    number_of_phases = len(phase_probabilities)
    base_phase_size = number_of_events // number_of_phases

    events: list[str] = []

    for phase_index, probabilities in enumerate(phase_probabilities):
        if phase_index == number_of_phases - 1:
            phase_size = number_of_events - len(events)
        else:
            phase_size = base_phase_size

        events.extend(
            rng.choices(
                EVENT_TYPES,
                weights=probabilities,
                k=phase_size,
            )
        )

    return events


def save_stream(events: Sequence[str], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["event_id", "event_type"])

        for event_id, event_type in enumerate(events):
            writer.writerow([event_id, event_type])


def main() -> None:
    number_of_events = 1_000_000
    output_directory = Path("generated_datasets")

    uniform = [0.10] * 10

    skewed_a = [
        0.55,
        0.05, 0.05, 0.05, 0.05,
        0.05, 0.05, 0.05, 0.05, 0.05,
    ]

    abc_dominant = [
        0.25, 0.25, 0.25,
        0.035714, 0.035714, 0.035714, 0.035714,
        0.035714, 0.035714, 0.035714,
    ]
    abc_dominant[-1] += 1.0 - sum(abc_dominant)

    abc_rare = [
        0.01, 0.01, 0.01,
        0.138571, 0.138571, 0.138571, 0.138571,
        0.138571, 0.138571, 0.138571,
    ]
    abc_rare[-1] += 1.0 - sum(abc_rare)

    datasets = {
        "uniform": generate_iid_stream(
            number_of_events,
            uniform,
            seed=1001,
        ),
        "skewed_A": generate_iid_stream(
            number_of_events,
            skewed_a,
            seed=1002,
        ),
        "ABC_dominant": generate_iid_stream(
            number_of_events,
            abc_dominant,
            seed=1003,
        ),
        "ABC_rare": generate_iid_stream(
            number_of_events,
            abc_rare,
            seed=1004,
        ),
        "ABC_correlated": generate_correlated_abc_stream(
            number_of_events,
            pattern_probability=0.08,
            noise_probabilities=uniform,
            seed=1005,
        ),
        "A_bursty": generate_bursty_stream(
            number_of_events,
            background_probabilities=uniform,
            burst_type="A",
            burst_length=50,
            burst_probability=0.005,
            seed=1006,
        ),
        "distribution_shift": generate_phased_stream(
            number_of_events,
            phase_probabilities=[
                uniform,
                abc_dominant,
                skewed_a,
            ],
            seed=1007,
        ),
    }

    for dataset_name, events in datasets.items():
        save_stream(
            events,
            output_directory / f"{dataset_name}_{number_of_events}.csv",
        )


if __name__ == "__main__":
    main()
