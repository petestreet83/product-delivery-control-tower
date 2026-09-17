from __future__ import annotations

import argparse

from .connectors import OpenMeteoConnector
from .contract import TemporalUpdateContract
from .orchestrator import UpdateOrchestrator
from .server import APIServer
from .storage import TemporalStore


def main():
    parser = argparse.ArgumentParser(description="Product delivery control tower API")
    parser.add_argument("--db-path", default="control_tower.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--latitude", type=float, default=37.7749)
    parser.add_argument("--longitude", type=float, default=-122.4194)
    parser.add_argument("--cadence-seconds", type=int, default=60)
    args = parser.parse_args()

    contract = TemporalUpdateContract(cadence_seconds=args.cadence_seconds)
    store = TemporalStore(args.db_path)
    connector = OpenMeteoConnector(latitude=args.latitude, longitude=args.longitude)
    orchestrator = UpdateOrchestrator(store=store, connectors=[connector], contract=contract)
    orchestrator.start_schedule()

    api = APIServer(host=args.host, port=args.port, store=store, orchestrator=orchestrator, contract=contract)
    try:
        api.serve_forever()
    finally:
        orchestrator.stop_schedule()


if __name__ == "__main__":
    main()
