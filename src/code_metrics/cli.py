"""Command-line entrypoints for the code_metrics package."""

import argparse
import runpy


def _run_seed_mongo() -> None:
    from code_metrics.storage.init_mongo import init_mongodb

    init_mongodb()


def _run_batch() -> None:
    from code_metrics.processing.batch_etl import run_batch_job

    run_batch_job()


def _run_simulator() -> None:
    from code_metrics.simulator.generate_logs import generate_telemetry, load_entities_from_mongo

    users, problems = load_entities_from_mongo()
    generate_telemetry(users, problems)


def _run_stream() -> None:
    runpy.run_module("code_metrics.processing.stream_leaderboard", run_name="__main__")


def _run_dashboard() -> None:
    runpy.run_module("code_metrics.dashboard.app", run_name="__main__")


def main() -> None:
    parser = argparse.ArgumentParser(description="Code Metrics Platform CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("seed-mongo", help="Seed MongoDB collections")
    sub.add_parser("batch", help="Run batch ETL job")
    sub.add_parser("simulate", help="Run telemetry simulator")
    sub.add_parser("stream", help="Run streaming leaderboard pipeline")
    sub.add_parser("dashboard", help="Run Streamlit dashboard module")

    args = parser.parse_args()

    if args.command == "seed-mongo":
        _run_seed_mongo()
        return
    if args.command == "batch":
        _run_batch()
        return
    if args.command == "simulate":
        _run_simulator()
        return
    if args.command == "stream":
        _run_stream()
        return
    if args.command == "dashboard":
        _run_dashboard()
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
