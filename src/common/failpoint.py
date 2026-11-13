"""Failpoints: crash a job at a named point, for the reprocessing experiments.

    failpoint("silver.mid_move")    # exits hard here when CHAOS_CRASH_AT=silver.mid_move

The process ends with os._exit, as after a kill -9 or a lost machine: no `finally` blocks, no
cleanup, no final report. Unset (normal runs) it does nothing. docs/bigdata-experiments.md
lists the points.
"""
import os


def failpoint(name):
    if os.getenv("CHAOS_CRASH_AT") == name:
        print(f"failpoint {name}: crashing now (CHAOS_CRASH_AT)", flush=True)
        os._exit(137)
