"""Aptem tap class."""

from __future__ import annotations

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from typing_extensions import override

from tap_aptem import metadata
from tap_aptem.client import AptemODataStream, EmbeddedCollectionStream

STREAM_REPLICATION_KEYS = {
    "AimWorkPlacements": "WorkPlaceStartDate",
    "ApprenticeshipFinancialRecords": "Date",
    "AptemCognitiveAssessments": "DateStart",
    "Actions": "UpdatedDate",
    "AwardingBodyQualifications": "UpdatedDate",
    "AwardingBodyQualificationsAssessmentHistory": "Date",
    "CheckpointAssessments": "LastModifiedDate",
    "ComplianceDocuments": "Date",
    "ComponentDueDateChanges": "ChangeDate",
    "CrmActivities": "UpdatedDate",
    "DestinationProgressions": "StartDate",
    "EmployerGroups": "UpdatedDate",
    "EPAErrors": "Date",
    "Episodes": "UpdatedDate",
    "Groups": "UpdatedDate",
    "HoursRecords": None,
    "IlrAims": "UpdatedDate",
    "IlrLearner": "UpdatedDate",
    "Jobs": "UpdatedDate",
    "LearnerEmployment": "EmploymentStartDate",
    "LearningPlanComponents": "UpdatedDate",
    "LearningPlanEvidences": "SubmissionDate",
    "LearningPlanEvidenceWithActivityData": "SubmissionDate",
    "MaximumProgrammeDataFields": None,
    "Messages": "CreatedDate",
    "Milestones": None,
    "Notes": "Date",
    "OnboardingResponses": "Date",
    "OrganizationContacts": None,
    "Organizations": "UpdatedDate",
    "ReviewResponses": "UpdatedDate",
    "Reviews": None,
    "Tasks": "UpdatedDate",
    "Trackers": "UpdatedDate",
    "Users": "UpdatedDate",
    "UserGroups": None,
    "VirtualAssistantExchanges": "Date",
    "WithdrawalReasons": "DateAdded",
}


class TapAptem(Tap):
    """Singer tap for the Aptem OData API."""

    name = "tap-aptem"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_token",
            th.StringType,
            required=True,
            description="API token for the Aptem OData API.",
        ),
        th.Property(
            "tenant_name",
            th.StringType,
            required=True,
            description="Aptem tenant name used to build the base URL.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for incremental replication.",
        ),
        th.Property(
            "odata_version",
            th.StringType,
            allowed_values=("1.0", "2.0"),
            default="1.0",
            description=(
                "Aptem OData API version to discover and query, e.g. '1.0' "
                "(the default) or '2.0'. Aptem publishes different entities on "
                "different versions, so a tenant with data on both versions "
                "needs one tap-aptem instance configured per version."
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self):
        tenant_name = self.config["tenant_name"]
        odata_version = self.config["odata_version"]
        url = f"https://{tenant_name}.aptem.co.uk/odata/{odata_version}/$metadata"

        response = requests.get(url, timeout=300)
        response.raise_for_status()

        for entity in metadata.discover_entities(response.text):
            if entity.parent_collection_name:
                stream_cls = EmbeddedCollectionStream
                path = f"/{entity.parent_collection_name}"
                kwargs = {"parent_key_map": entity.parent_key_map}

            else:
                stream_cls = AptemODataStream
                path = f"/{entity.collection_name}"
                kwargs = {}

            stream = stream_cls(
                tap=self,
                name=entity.collection_name,
                schema=entity.jsonschema,
                path=path,
                **kwargs,
            )

            stream.primary_keys = entity.primary_keys

            try:
                replication_key = STREAM_REPLICATION_KEYS[stream.name]
            except KeyError:
                if type(stream) is AptemODataStream:
                    self.logger.warning(
                        "No replication key defined for %s",
                        stream.name,
                    )

                replication_key = None

            stream.replication_key = replication_key

            yield stream


if __name__ == "__main__":
    TapAptem.cli()
