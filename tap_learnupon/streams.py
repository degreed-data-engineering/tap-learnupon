"""Stream class for tap-learnupon."""

import logging
import requests

from typing import Optional, Any, Dict
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BasicAuthenticator


logging.basicConfig(level=logging.INFO)


class TapLearnuponStream(RESTStream):
    """Generic Learnupon stream class."""

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return self.config["url_base"]

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
