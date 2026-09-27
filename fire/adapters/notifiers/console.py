"""Console notifier for local demos."""

from __future__ import annotations

import sys
from typing import TextIO

from fire.core.domain.incident import Incident
from fire.core.domain.narration import Narration


class ConsoleNotifier:
    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream or sys.stdout

    async def publish(self, incident: Incident, narration: Narration) -> None:
        print(
            f"[fire] notify incident={incident.id}\n{narration.channel_text}\n",
            file=self._stream,
        )
