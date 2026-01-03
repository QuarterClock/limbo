"""Plugin markers for limbo hook specifications and implementations.

This module provides the markers used to define hook specifications
and implementations in the limbo plugin system.

Example:
    Defining a hook specification::

        from limbo_core.plugins.markers import hookspec

        class MyHookSpec:
            @hookspec
            def my_hook(self, arg: str) -> str:
                '''My hook specification.'''

    Implementing a hook::

        from limbo_core.plugins.markers import hookimpl

        class MyPlugin:
            @hookimpl
            def my_hook(self, arg: str) -> str:
                return f"processed: {arg}"
"""

import pluggy

PROJECT_NAME = "limbo"

hookspec = pluggy.HookspecMarker(PROJECT_NAME)
hookimpl = pluggy.HookimplMarker(PROJECT_NAME)
