"""Temporary local ingress runner for controlled webhook/spool trials."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from wsgiref.simple_server import make_server

from gateway.http_ingress import create_app
from gateway.ingress_auth import DUMMY_WEBHOOK_SECRET
from gateway.spool import GatewaySpool

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
_PUBLIC_HOSTS = {"0.0.0.0", "::", ""}


@dataclass(frozen=True)
class IngressRunnerConfig:
    host: str
    port: int
    spool_db: Path | None
    profile: str
    shared_secret: str | None
    allow_public_bind: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run temporary local Telegram ingress WSGI app")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Bind host; defaults to loopback")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Bind TCP port")
    parser.add_argument("--spool-db", type=Path, default=None, help="SQLite spool DB path")
    parser.add_argument("--profile", default="naval", help="Target profile label for local run")
    parser.add_argument("--shared-secret", default=None, help="Expected shared secret label/value for operator tracking")
    parser.add_argument("--allow-public-bind", action="store_true", help="Allow binding non-loopback/public host")
    return parser


def parse_args(argv: list[str] | None = None) -> IngressRunnerConfig:
    args = build_parser().parse_args(argv)
    return IngressRunnerConfig(
        host=args.host,
        port=args.port,
        spool_db=args.spool_db,
        profile=args.profile,
        shared_secret=args.shared_secret,
        allow_public_bind=args.allow_public_bind,
    )


def is_public_bind(host: str) -> bool:
    normalized = host.strip().lower()
    if normalized in _PUBLIC_HOSTS:
        return True
    if normalized.startswith("127.") or normalized == "localhost" or normalized == "::1":
        return False
    return True


def validate_config(config: IngressRunnerConfig) -> None:
    if config.port < 1 or config.port > 65535:
        raise ValueError("port must be between 1 and 65535")
    if is_public_bind(config.host) and not config.allow_public_bind:
        raise ValueError("refusing public bind without --allow-public-bind")


def build_app(config: IngressRunnerConfig):
    validate_config(config)
    spool = GatewaySpool(config.spool_db) if config.spool_db is not None else GatewaySpool()
    spool.initialize()
    return create_app(spool=spool)


def run(config: IngressRunnerConfig) -> None:
    """Run the temporary WSGI server until interrupted by the caller/operator."""

    app = build_app(config)
    with make_server(config.host, config.port, app) as httpd:
        httpd.serve_forever()


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    validate_config(config)
    # Do not print secrets. Only acknowledge whether a non-default secret was supplied.
    secret_mode = "provided" if config.shared_secret and config.shared_secret != DUMMY_WEBHOOK_SECRET else "default-or-empty"
    print(
        f"Starting temporary ingress on {config.host}:{config.port} "
        f"profile={config.profile} spool_db={config.spool_db or 'default'} shared_secret={secret_mode}"
    )
    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover - manual operator entry point only
    raise SystemExit(main())
