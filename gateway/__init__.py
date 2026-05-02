"""
Hermes Gateway - Multi-platform messaging integration.

This package exposes gateway config/session/delivery helpers lazily so importing
lightweight submodules (for example polling watchers) does not eagerly load the
full gateway runtime.
"""

_CONFIG_EXPORTS = {"GatewayConfig", "PlatformConfig", "HomeChannel", "load_gateway_config"}
_SESSION_EXPORTS = {"SessionContext", "SessionStore", "SessionResetPolicy", "build_session_context_prompt"}
_DELIVERY_EXPORTS = {"DeliveryRouter", "DeliveryTarget"}

__all__ = [
    # Config
    "GatewayConfig",
    "PlatformConfig",
    "HomeChannel",
    "load_gateway_config",
    # Session
    "SessionContext",
    "SessionStore",
    "SessionResetPolicy",
    "build_session_context_prompt",
    # Delivery
    "DeliveryRouter",
    "DeliveryTarget",
]


def __getattr__(name: str):
    if name in _CONFIG_EXPORTS:
        from . import config

        return getattr(config, name)
    if name in _SESSION_EXPORTS:
        from . import session

        return getattr(session, name)
    if name in _DELIVERY_EXPORTS:
        from . import delivery

        return getattr(delivery, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
