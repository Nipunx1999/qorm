"""Command-line interface for qorm.

Usage::

    qorm generate --host HOST --port PORT --tables t1,t2 [--output DIR]
    qorm generate --service NAME --market M --env E --tables t1,t2
    python -m qorm generate ...
"""

from __future__ import annotations

import argparse
import sys


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qorm",
        description="qorm CLI — tools for working with kdb+ from Python.",
    )
    sub = parser.add_subparsers(dest="command")

    gen = sub.add_parser(
        "generate",
        help="Introspect kdb+ tables and generate typed model files.",
    )

    # Connection: host+port OR service
    conn = gen.add_argument_group("connection")
    conn.add_argument("--host", help="kdb+ hostname")
    conn.add_argument("--port", type=int, help="kdb+ port")
    conn.add_argument(
        "--service",
        help="QNS service name (DATASET.CLUSTER.DBTYPE.NODE)",
    )
    conn.add_argument("--market", help="QNS market (required with --service)")
    conn.add_argument("--env", help="QNS environment (required with --service)")
    conn.add_argument("--user", default="", help="kdb+ username")
    conn.add_argument("--password", default="", help="kdb+ password")
    conn.add_argument(
        "--tls", action="store_true", default=False,
        help="Enable TLS (for --host/--port connections).",
    )
    conn.add_argument(
        "--tls-no-verify", action="store_true", default=False,
        help="Disable TLS certificate verification (self-signed certs).",
    )

    # What to generate
    gen.add_argument(
        "--tables",
        required=True,
        help="Comma-separated table names to introspect.",
    )
    gen.add_argument(
        "--output",
        default="./models",
        help="Output directory for generated model files (default: ./models).",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "generate":
        return _cmd_generate(args)

    return 0


def _cmd_generate(args: argparse.Namespace) -> int:
    from .engine import Engine
    from .codegen import generate_models

    # Build engine from either host+port or service
    if args.service:
        if not args.market or not args.env:
            print("error: --market and --env are required with --service", file=sys.stderr)
            return 1
        engine = Engine.from_service(
            args.service,
            market=args.market,
            env=args.env,
            username=args.user,
            password=args.password,
        )
    elif args.host and args.port:
        engine = Engine(
            host=args.host,
            port=args.port,
            username=args.user,
            password=args.password,
            tls=args.tls,
        )
    else:
        print("error: provide either --host/--port or --service", file=sys.stderr)
        return 1

    if args.tls_no_verify:
        import ssl
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # Relax cipher requirements for kdb+ TLS compatibility
        try:
            ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
        except ssl.SSLError:
            ctx.set_ciphers("DEFAULT")
        engine.tls = True
        engine.tls_context = ctx

    table_names = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not table_names:
        print("error: --tables must list at least one table name", file=sys.stderr)
        return 1

    try:
        generated = generate_models(engine, args.output, table_names)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    for path in generated:
        print(f"  wrote {path}")

    print(f"\nGenerated {len(generated)} file(s) in {args.output}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
