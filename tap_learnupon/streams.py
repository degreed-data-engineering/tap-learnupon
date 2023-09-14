"""Stream class for tap-learnupon."""

import logging
import requests

from typing import Optional, Any, Dict, Iterable
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BasicAuthenticator
import itertools

logging.basicConfig(level=logging.INFO)


class TapLearnuponStream(RESTStream):
    """Generic Learnupon stream class."""

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return f"https://{self.config.get('domain')}.{self.config.get('url_base')}/api/v1"

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("username"),
            password=self.config.get("password"),
        )

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        has_next_page = response.headers.get("LU-Has-Next-Page", "false")
        if has_next_page == "true":
            current_page = response.headers.get("LU-Current-Page", 0)
            next_page = current_page + 1
            next_page_token = next_page
            return next_page_token
        else:
            next_page_token = None
            return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        return params


class LearningPaths(TapLearnuponStream):
    selected_by_default = False
    name = "learning_paths"  # Stream name
    path = "/learning_paths"  # API endpoint after base_url
    records_jsonpath = "$.learning_paths[0:]"  # https://jsonpath.com Use requests response json to identify the json path
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("sellable", th.BooleanType),
        th.Property("cataloged", th.BooleanType),
        th.Property("keywords", th.StringType),
        th.Property("due_days_after_enrollment", th.IntegerType),
        th.Property("send_due_date_reminders", th.BooleanType),
        th.Property("due_date_reminder_days", th.IntegerType),
        th.Property("due_date_reminder_days_2", th.IntegerType),
        th.Property("minute_length", th.NumberType),
        th.Property("path_length_unit", th.StringType),
        th.Property("price", th.IntegerType),
        th.Property("published_status_id", th.StringType),
        th.Property("difficulty_level", th.StringType),
        th.Property("description_html", th.StringType),
        th.Property("description_text", th.StringType),
        th.Property("thumbnail_image_url", th.StringType),
        th.Property("credits_to_be_awarded", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("date_published", th.DateTimeType),
        th.Property("due_date_after_enrollment", th.DateTimeType),
    ).to_dict()


class Courses(TapLearnuponStream):
    selected_by_default = False
    name = "courses"  # Stream name
    path = "/courses"  # API endpoint after base_url
    records_jsonpath = "$.courses[0:]"  # https://jsonpath.com Use requests response json to identify the json path
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("version", th.StringType),
        th.Property("source_id", th.IntegerType),
        th.Property("created_at", th.DateTimeType),
        th.Property("sellable", th.BooleanType),
        th.Property("cataloged", th.BooleanType),
        th.Property("date_published", th.DateTimeType),
        th.Property("keywords", th.StringType),
        th.Property("reference_code", th.StringType),
        th.Property("manager_can_enroll", th.BooleanType),
        th.Property("allow_users_rate_course", th.BooleanType),
        th.Property("number_of_reviews", th.IntegerType),
        th.Property("number_of_stars", th.IntegerType),
        th.Property("minute_length", th.NumberType),
        th.Property("course_length_unit", th.StringType),
        th.Property("num_enrolled", th.IntegerType),
        th.Property("num_not_started", th.IntegerType),
        th.Property("num_in_progress", th.IntegerType),
        th.Property("num_completed", th.IntegerType),
        th.Property("num_passed", th.IntegerType),
        th.Property("num_failed", th.IntegerType),
        th.Property("num_pending_review", th.IntegerType),
        th.Property("number_of_modules", th.IntegerType),
        th.Property("price", th.IntegerType),
        th.Property("published_status_id", th.StringType),
        th.Property("difficulty_level", th.StringType),
        th.Property("description_html", th.StringType),
        th.Property("description_text", th.StringType),
        th.Property("objectives_html", th.StringType),
        th.Property("objectives_text", th.StringType),
        th.Property("credits_to_be_awarded", th.StringType),
        th.Property("due_days_after_enrollment", th.IntegerType),
        th.Property("due_date_after_enrollment", th.DateTimeType),
        th.Property("send_due_date_reminders", th.BooleanType),
        th.Property("due_date_reminder_days", th.IntegerType),
        th.Property("due_date_reminder_days_2", th.IntegerType),
        th.Property("thumbnail_image_url", th.StringType),
        th.Property("license_expires", th.DateTimeType),
        th.Property("license_number_enrollments_purchased", th.IntegerType),
        th.Property("license_is_open_ended", th.BooleanType),
        th.Property("license_has_unlimited_enrollments", th.BooleanType),
        th.Property("owner_first_name", th.StringType),
        th.Property("owner_last_name", th.StringType),
        th.Property("owner_email", th.StringType),
        th.Property("owner_id", th.IntegerType),
        th.Property("learning_awards", th.StringType),
        th.Property("customDataFieldValues", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"course_id": record["id"]}


class Modules(TapLearnuponStream):
    selected_by_default = False
    parent_stream_type = Courses
    name = "modules"  # Stream name
    path = "/modules?course_id={course_id}"  # API endpoint after base_url
    records_jsonpath = "$.modules[0:]"  # https://jsonpath.com Use requests response json to identify the json path
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("tags", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("exam_id", th.IntegerType),
        th.Property("number_of_linked_courses", th.IntegerType),
        th.Property("component_type", th.StringType),
        th.Property("description_html", th.StringType),
        th.Property("description_text", th.StringType),
        th.Property("creator_id", th.IntegerType),
        th.Property("creator_first_name", th.StringType),
        th.Property("creator_last_name", th.StringType),
        th.Property("creator_email", th.StringType),
        th.Property("creator_username", th.StringType),
        th.Property("assignment_passing_percentage", th.IntegerType),
        th.Property("assignment_question_html", th.StringType),
        th.Property("assignment_question_text", th.StringType),
        th.Property("location_id", th.IntegerType),
        th.Property("location", th.StringType),
        th.Property("address_1", th.StringType),
        th.Property("address_2", th.StringType),
        th.Property("address_3", th.StringType),
        th.Property("location_state_code", th.StringType),
        th.Property("location_country_code", th.StringType),
        th.Property("start_at", th.DateTimeType),
        th.Property("end_at", th.DateTimeType),
        th.Property("timezone", th.StringType),
        th.Property("number_enrolled_on_session", th.IntegerType),
        th.Property("max_capacity", th.IntegerType),
        th.Property("session_tutor_id", th.IntegerType),
        th.Property("tutor_first_name", th.StringType),
        th.Property("tutor_last_name", th.StringType),
        th.Property("tutor_email", th.StringType),
        th.Property("tutor_username", th.StringType),
        th.Property("training_id", th.IntegerType),
        th.Property("session_id", th.IntegerType),
    ).to_dict()


class Unified(TapLearnuponStream):
    selected_by_default = True
    name = "unified"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("cataloged", th.BooleanType),
        th.Property("date_published", th.DateTimeType),
        th.Property("keywords", th.StringType),
        th.Property("course_length_unit", th.StringType),
        th.Property("minute_length", th.NumberType),
        th.Property("published_status_id", th.StringType),
        th.Property("difficulty_level", th.StringType),
        th.Property("description_text", th.StringType),
        th.Property("credits_to_be_awarded", th.StringType),
        th.Property("thumbnail_image_url", th.StringType),
        th.Property("stream", th.StringType),
    ).to_dict()

    def __init__(self, tap=None, **kwargs):
        super().__init__(tap=tap, **kwargs)

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        # Fetch records from the other streams
        learning_paths = LearningPaths(tap=self._tap)
        courses = Courses(tap=self._tap)
        modules = Modules(tap=self._tap)

        # Combine the records into a unified format
        for stream, records in [('learning_paths', learning_paths.get_records(context)),
                                ('courses', courses.get_records(context)),
                                ('modules', modules.get_records(context))]:
            for record in records:
                yield self.transform_record(record, stream)


    def transform_record(self, record: dict, stream: str) -> dict:
        # Define the unified schema
        unified_schema = {
            "id": None,
            "name": None,
            "created_at": None,
            "cataloged": None,
            "date_published": None,
            "keywords": None,
            "minute_length": None,
            "course_length_unit": None,
            "published_status_id": None,
            "difficulty_level": None,
            "description_text": None,
            "credits_to_be_awarded": None,
            "thumbnail_image_url": None,
            "stream": None
        }

        # Update the unified record with data from the original record
        for key in record.keys():
            if key in unified_schema:
                unified_schema[key] = record[key]

        # Indicate the source stream
        unified_schema["stream"] = stream

        return unified_schema
