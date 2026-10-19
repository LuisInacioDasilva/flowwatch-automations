import argparse
import json
import time
from pathlib import Path

from .engine import Store
from .report import write_report


def main():
    parser = argparse.ArgumentParser(description="Flowwatch: fila local e relatório de automações")
    parser.add_argument("--db", default="data/flowwatch.sqlite", help="arquivo SQLite local")
    sub = parser.add_subparsers(dest="command", required=True)
    enqueue = sub.add_parser("enqueue")
    enqueue.add_argument("--key", required=True)
    enqueue.add_argument("--type", required=True, choices=["normalize_contact", "draft_summary"])
    enqueue.add_argument("--payload", required=True, help="JSON do payload")
    enqueue.add_argument("--max-attempts", type=int, default=3)
    sub.add_parser("run-once")
    worker = sub.add_parser("worker")
    worker.add_argument("--interval", type=float, default=2)
    sub.add_parser("metrics")
    requeue = sub.add_parser("requeue-dead")
    requeue.add_argument("--job-id", type=int, required=True)
    requeue.add_argument("--actor", required=True)
    requeue.add_argument("--reason", required=True)
    report = sub.add_parser("report")
    report.add_argument("--output", default="dist/index.html")
    demo = sub.add_parser("demo")
    demo.add_argument("--output", default="dist/index.html")
    args = parser.parse_args()

    if args.command == "demo":
        store = Store()
        now = int(time.time())
        store.enqueue(key="demo-contact-1", task_type="normalize_contact", payload={"name": "Ana Lima", "phone": "+55 (11) 99999-1234"}, now=now)
        store.enqueue(key="demo-summary-1", task_type="draft_summary", payload={"amounts_cents": [12990, 24800, 7850]}, now=now)
        store.enqueue(key="demo-broken-1", task_type="normalize_contact", payload={"name": "Sem número", "phone": "123"}, now=now, max_attempts=2)
        store.enqueue(key="demo-contact-2", task_type="normalize_contact", payload={"name": "Bruno Reis", "phone": "11988887777"}, now=now)
        for step in range(4):
            while store.process_one(now=now + step * 10):
                pass
        print(f"Relatório: {write_report(store, args.output)}")
        store.close()
        return

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = Store(db_path)
    try:
        if args.command == "enqueue":
            job_id = store.enqueue(key=args.key, task_type=args.type, payload=json.loads(args.payload), max_attempts=args.max_attempts)
            print(f"Job #{job_id}")
        elif args.command == "run-once":
            print("executado" if store.process_one() else "nenhum job pronto")
        elif args.command == "worker":
            print("Worker ativo; Ctrl+C para parar")
            try:
                while True:
                    if not store.process_one():
                        time.sleep(args.interval)
            except KeyboardInterrupt:
                pass
        elif args.command == "report":
            print(f"Relatório: {write_report(store, args.output)}")
        elif args.command == "metrics":
            print(json.dumps(store.metrics(), ensure_ascii=False, indent=2))
        elif args.command == "requeue-dead":
            new_id = store.requeue_dead(args.job_id, actor=args.actor, reason=args.reason)
            print(f"Job #{args.job_id} preservado; recuperação autorizada como job #{new_id}")
    finally:
        store.close()


if __name__ == "__main__":
    main()
