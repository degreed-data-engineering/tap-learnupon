"""learnupon tap class."""

from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_learnupon.streams import LearningPaths, Courses, Modules, Unified

PLUGIN_NAME = "tap-learnupon"

STREAM_TYPES = [LearningPaths, Courses, Modules,Unified]


class TapLearnupon(Tap):
    """learnupon tap class."""

    name = "tap-learnupon"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "url_base",
            th.StringType,
            required=True,
            description="Url base for the source endpoint",
        ),
        th.Property("username", th.StringType, required=True, description="Username"),
        th.Property("password", th.StringType, required=True, description="Password"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        return streams


# CLI Execution:
cli = TapLearnupon.cli
