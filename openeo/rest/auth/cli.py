import argparse
import builtins
import json
import logging
import sys
from collections import OrderedDict
from getpass import getpass
from pathlib import Path
from typing import List, Tuple

from openeo import Connection, connect
from openeo.capabilities import ApiVersionException
from openeo.rest.auth.config import AuthConfig, RefreshTokenStore
from openeo.rest.auth.oidc import OidcProviderInfo

_log = logging.getLogger(__name__)


class CliToolException(RuntimeError):
    pass


_OIDC_FLOW_CHOICES = [
    "auth-code",
    "device",
    # TODO: add client credentials flow?
]


def main(argv=None):
    root_parser = argparse.ArgumentParser(
        description="Tool to manage openEO related authentication and configuration."
    )
    root_parser.add_argument(
        "--verbose", "-v", action="count", default=0,
        help="Increase logging verbosity. Can be given multiple times."
    )
    root_subparsers = root_parser.add_subparsers(title="Subcommands", dest="subparser_name")

    # Command: paths
    paths_parser = root_subparsers.add_parser(
        "paths", help="Show paths to config/token files."
    )
    paths_parser.set_defaults(func=main_paths)

    # Command: config-dump
    config_dump_parser = root_subparsers.add_parser(
        "config-dump", help="Dump config file.", aliases=["config"]
    )
    config_dump_parser.set_defaults(func=main_config_dump)
    config_dump_parser.add_argument("--show-secrets", action="store_true", help="Don't redact secrets in the dump.")

    # Command: token-dump
    token_dump_parser = root_subparsers.add_parser(
        "token-dump", help="Dump OpenID Connect refresh tokens file.", aliases=["tokens"]
    )
    token_dump_parser.set_defaults(func=main_token_dump)
    token_dump_parser.add_argument("--show-secrets", action="store_true", help="Don't redact secrets in the dump.")

    # Command: token-clear
    token_clear_parser = root_subparsers.add_parser(
        "token-clear", help="Remove OpenID Connect refresh tokens file."
    )
    token_clear_parser.set_defaults(func=main_token_clear)
    token_clear_parser.add_argument("--force", "-f", action="store_true", help="Remove without asking confirmation.")

    # Command: add-basic
    add_basic_parser = root_subparsers.add_parser(
        "add-basic", help="Add or update config entry for basic auth."
    )
    add_basic_parser.set_defaults(func=main_add_basic)
    add_basic_parser.add_argument("backend", help="OpenEO Backend URL.")
    add_basic_parser.add_argument("--username", help="Basic auth username.")
    add_basic_parser.add_argument(
        "--no-try", dest="try_auth", action="store_false",
        help="Don't try out the credentials against the backend, just store them."
    )

    # Command: add-oidc
    add_oidc_parser = root_subparsers.add_parser(
        "add-oidc", help="Add or update config entry for OpenID Connect."
    )
    add_oidc_parser.set_defaults(func=main_add_oidc)
    add_oidc_parser.add_argument("backend", help="OpenEO Backend URL.")
    add_oidc_parser.add_argument("--provider-id", help="Provider ID to use.")
    add_oidc_parser.add_argument("--client-id", help="Client ID to use.")
    add_oidc_parser.add_argument(
        "--no-client-secret", dest="ask_client_secret", default=True, action="store_false",
        help="Don't ask for secret (because client does not need one)."
    )
    add_oidc_parser.add_argument(
        "--use-default-client", action="store_true",
        help="Use default client (as provided by backend)."
    )

    # Command: oidc-auth
    oidc_auth_parser = root_subparsers.add_parser(
        "oidc-auth", help="Do OpenID Connect authentication flow and store refresh tokens."
    )
    oidc_auth_parser.set_defaults(func=main_oidc_auth)
    oidc_auth_parser.add_argument("backend", help="OpenEO Backend URL.")
    oidc_auth_parser.add_argument("--provider-id", help="Provider ID to use.")
    oidc_auth_parser.add_argument(
        "--flow", choices=_OIDC_FLOW_CHOICES, default="device",
        help="OpenID Connect flow to use (default: device)."
    )
    oidc_auth_parser.add_argument(
        "--timeout", type=int, default=60, help="Timeout in seconds to wait for (user) response."
    )

    # Parse arguments and execute sub-command
    args = root_parser.parse_args(argv)
    logging.basicConfig(level={0: logging.WARN, 1: logging.INFO}.get(args.verbose, logging.DEBUG))
    _log.debug(repr(args))
    if args.subparser_name:
        args.func(args)
    else:
        root_parser.print_help()


def main_paths(args):
    """
    Print paths of auth config file and refresh token cache file.
    """

    def describe(p: Path):
        if p.exists():
            return "perms: 0o{p:o}, size: {s}B".format(p=p.stat().st_mode & 0o777, s=p.stat().st_size)
        else:
            return "does not exist"

    config_path = AuthConfig().path
    print("openEO auth config: {p} ({d})".format(p=str(config_path), d=describe(config_path)))
    tokens_path = RefreshTokenStore().path
    print("openEO OpenID Connect refresh token store: {p} ({d})".format(p=str(tokens_path), d=describe(tokens_path)))


def _redact(d: dict, keys_to_redact: List[str]):
    """Redact secrets in given dict in-place."""
    for k, v in d.items():
        if k in keys_to_redact:
            d[k] = "<redacted>"
        elif isinstance(v, dict):
            _redact(v, keys_to_redact=keys_to_redact)


def main_config_dump(args):
    """
    Dump auth config file
    """
    config = AuthConfig()
    print("### {p} ".format(p=str(config.path)).ljust(80, "#"))
    data = config.load(empty_on_file_not_found=False)
    if not args.show_secrets:
        _redact(data, keys_to_redact=["client_secret", "password", "refresh_token"])
    json.dump(data, fp=sys.stdout, indent=2)
    print()


def main_token_dump(args):
    """
    Dump refresh token file
    """
    tokens = RefreshTokenStore()
    print("### {p} ".format(p=str(tokens.path)).ljust(80, "#"))
    data = tokens.load(empty_on_file_not_found=False)
    if not args.show_secrets:
        _redact(data, keys_to_redact=["client_secret", "password", "refresh_token"])
    json.dump(data, fp=sys.stdout, indent=2)
    print()


def main_token_clear(args):
    """
    Remove refresh token file
    """
    tokens = RefreshTokenStore()
    path = tokens.path
    if path.exists():
        if not args.force:
            answer = builtins.input(f"Remove refresh token file {path}? 'y' or 'n': ")
            if answer.lower()[:1] != "y":
                print("Keeping refresh token file.")
                return
        tokens.remove()
        print(f"Removed refresh token file {path}.")
    else:
        print(f"No refresh token file at {path}.")


def main_add_basic(args):
    """
    Add a config entry for basic auth
    """
    backend = args.backend
    username = args.username
    try_auth = args.try_auth
    config = AuthConfig()

    print("Will add basic auth config for backend URL {b!r}".format(b=backend))
    print("to config file: {c!r}".format(c=str(config.path)))

    # Find username and password
    if not username:
        username = builtins.input("Enter username and press enter: ")
    print("Using username {u!r}".format(u=username))
    password = getpass("Enter password and press enter: ") or None

    if try_auth:
        print("Trying to authenticate with {b!r}".format(b=backend))
        con = connect(backend)
        con.authenticate_basic(username, password)
        print("Successfully authenticated {u!r}".format(u=username))

    config.set_basic_auth(backend=backend, username=username, password=password)
    print("Saved credentials to {p!r}".format(p=str(config.path)))


def _interactive_choice(title: str, options: List[Tuple[str, str]], attempts=10) -> str:
    """
    Let user choose between options (given as dict) and return chosen key
    """
    print(title)
    for c, (k, v) in enumerate(options):
        print("[{c:d}] {v}".format(c=c + 1, v=v))
    for _ in range(attempts):
        try:
            entered = builtins.input("Choose one (enter index): ")
            return options[int(entered) - 1][0]
        except Exception:
            pass
    raise CliToolException("Failed to pick valid option.")


def show_warning(message: str):
    _log.warning(message)


def main_add_oidc(args):
    """
    Add a config entry for OIDC auth
    """
    backend = args.backend
    provider_id = args.provider_id
    client_id = args.client_id
    ask_client_secret = args.ask_client_secret
    use_default_client = args.use_default_client
    config = AuthConfig()

    print("Will add OpenID Connect auth config for backend URL {b!r}".format(b=backend))
    print("to config file: {c!r}".format(c=str(config.path)))

    con = connect(backend)
    con.capabilities().api_version_check.require_at_least("1.0.0")

    # Find provider ID
    oidc_info = con.get("/credentials/oidc", expected_status=200).json()
    providers = OrderedDict((p["id"], OidcProviderInfo.from_dict(p)) for p in oidc_info["providers"])

    if not providers:
        raise CliToolException("No OpenID Connect providers listed by backend {b!r}.".format(b=backend))
    if not provider_id:
        if len(providers) == 1:
            provider_id = list(providers.keys())[0]
        else:
            provider_id = _interactive_choice(
                title="Backend {b!r} has multiple OpenID Connect providers.".format(b=backend),
                options=[(p.id, "{t} (issuer {s})".format(t=p.title, s=p.issuer)) for p in providers.values()]
            )
    if provider_id not in providers:
        raise CliToolException("Invalid provider ID {p!r}. Should be one of {o}.".format(
            p=provider_id, o=list(providers.keys())
        ))
    provider = providers[provider_id]
    print("Using provider ID {p!r} (issuer {i!r})".format(p=provider_id, i=provider.issuer))

    # Get client_id and client_secret (if necessary)
    if use_default_client:
        if not provider.default_clients:
            show_warning("No default clients declared for provider {p!r}".format(p=provider_id))
        client_id, client_secret = None, None
    else:
        if not client_id:
            if provider.default_clients:
                client_prompt = "Enter client_id or leave empty to use default client, and press enter: "
            else:
                client_prompt = "Enter client_id and press enter: "
            client_id = builtins.input(client_prompt).strip() or None
        print("Using client ID {u!r}".format(u=client_id))
        if not client_id and not provider.default_clients:
            show_warning("Given client ID was empty.")

        if client_id and ask_client_secret:
            client_secret = getpass("Enter client_secret or leave empty to not use a secret, and press enter: ") or None
        else:
            client_secret = None

    config.set_oidc_client_config(
        backend=backend, provider_id=provider_id, client_id=client_id, client_secret=client_secret,
        issuer=provider.issuer
    )
    print("Saved client information to {p!r}".format(p=str(config.path)))


_webbrowser_open = None


def main_oidc_auth(args):
    """
    Do OIDC auth flow and store refresh tokens.
    """
    backend = args.backend
    oidc_flow = args.flow
    provider_id = args.provider_id
    timeout = args.timeout

    config = AuthConfig()

    print("Will do OpenID Connect flow to authenticate with backend {b!r}.".format(b=backend))
    print("Using config {c!r}.".format(c=str(config.path)))

    # Determine provider
    provider_configs = config.get_oidc_provider_configs(backend=backend)
    _log.debug("Provider configs: {c!r}".format(c=provider_configs))
    if not provider_id:
        if len(provider_configs) == 0:
            print("Will try to use default provider_id.")
            provider_id = None
        elif len(provider_configs) == 1:
            provider_id = list(provider_configs.keys())[0]
        else:
            provider_id = _interactive_choice(
                title="Multiple OpenID Connect providers available for backend {b!r}".format(b=backend),
                options=sorted(
                    (k, "{k}: issuer {s}".format(k=k, s=v.get("issuer", "n/a")))
                    for k, v in provider_configs.items()
                )
            )
    if not (provider_id is None or provider_id in provider_configs):
        raise CliToolException("Invalid provider ID {p!r}. Should be `None` or one of {o}.".format(
            p=provider_id, o=list(provider_configs.keys())
        ))
    print("Using provider ID {p!r}.".format(p=provider_id))

    # Get client id and secret
    client_id, client_secret = config.get_oidc_client_configs(backend=backend, provider_id=provider_id)
    if client_id:
        print("Using client ID {c!r}.".format(c=client_id))
    else:
        print("Will try to use default client.")

    refresh_token_store = RefreshTokenStore()
    con = Connection(backend, refresh_token_store=refresh_token_store)
    if oidc_flow == "auth-code":
        print("Starting OpenID Connect authorization code flow:")
        print("a browser window should open allowing you to log in with the identity provider\n"
              "and grant access to the client {c!r} (timeout: {t}s).".format(c=client_id, t=timeout))
        con.authenticate_oidc_authorization_code(
            client_id=client_id, client_secret=client_secret,
            provider_id=provider_id,
            timeout=timeout,
            store_refresh_token=True,
            webbrowser_open=_webbrowser_open
        )
        print("The OpenID Connect authorization code flow was successful.")
    elif oidc_flow == "device":
        print("Starting OpenID Connect device flow.")
        con.authenticate_oidc_device(
            client_id=client_id, client_secret=client_secret,
            provider_id=provider_id,
            store_refresh_token=True
        )
        print("The OpenID Connect device flow was successful.")
    else:
        raise CliToolException("Invalid flow {f!r}".format(f=oidc_flow))

    print("Stored refresh token in {p!r}".format(p=str(refresh_token_store.path)))


if __name__ == '__main__':
    main()
