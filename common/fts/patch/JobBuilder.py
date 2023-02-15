#   Copyright 2015-2020 CERN
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import socket
import time

from datetime import datetime
from urllib.parse import urlparse, parse_qsl, ParseResult, urlencode
from flask import current_app as app
from .JobBuilder_utils import *

log = logging.getLogger(__name__)


class JobBuilder:
    """
    From a dictionary, build the internal representation of Job, Files and
    Data Management
    """

    def _get_params(self, submitted_params):
        """
        Returns proper parameters applying defaults and user values
        """
        params = dict()
        params.update(DEFAULT_PARAMS)
        params.update(submitted_params)
        # Some parameters may be explicitly None to pick the default, so re-apply defaults
        for k, v in params.items():
            if v is None and k in DEFAULT_PARAMS:
                params[k] = DEFAULT_PARAMS[k]

        # Enforce JSON type for 'job_metadata'
        if params["job_metadata"] is not None:
            params["job_metadata"] = metadata(params["job_metadata"])

        return params

    def _build_internal_job_params(self):
        """
        Generates the value for job.internal_job_params depending on the
        received protocol parameters
        """
        param_list = []
        if self.params.get("nostreams", None):
            param_list.append("nostreams:%d" % int(self.params["nostreams"]))
        if self.params.get("timeout", None):
            param_list.append("timeout:%d" % int(self.params["timeout"]))
        if self.params.get("buffer_size", None):
            param_list.append("buffersize:%d" % int(self.params["buffer_size"]))
        if self.params.get("strict_copy", False):
            param_list.append("strict")
        if self.params.get("ipv4", False):
            param_list.append("ipv4")
        elif self.params.get("ipv6", False):
            param_list.append("ipv6")
        if self.params.get("s3alternate", False):
            param_list.append("s3alternate")

        if len(param_list) == 0:
            return None
        else:
            return ",".join(param_list)

    def _set_job_source_and_destination(self, entries):
        """
        Iterates through the files that belong to the job, and determines the
        'overall' job source and destination Storage Elements
        """
        # Multihop
        if self.job["job_type"] == "H":
            self.job["source_se"] = entries[0]["source_se"]
            self.job["dest_se"] = entries[-1]["dest_se"]
        # Regular transfers
        else:
            self.job["source_se"] = entries[0]["source_se"]
            self.job["dest_se"] = entries[0]["dest_se"]
            for elem in entries:
                if elem["source_se"] != self.job["source_se"]:
                    self.job["source_se"] = None
                if elem["dest_se"] != self.job["dest_se"]:
                    self.job["dest_se"] = None

    def _set_activity_query_string(self, url, file_dict):
        """
        Set the activity query string in the given url
        """
        query_p = parse_qsl(url.query)
        query_p.append(("activity", file_dict.get("activity", "default")))
        query_str = urlencode(query_p)
        return ParseResult(
            scheme=url.scheme,
            netloc=url.netloc,
            path=url.path,
            params=url.params,
            query=query_str,
            fragment=url.fragment,
        )

    def _populate_files(self, file_dict, f_index, shared_hashed_id):
        """
        From the dictionary file_dict, generate a list of transfers for a job
        """
        # Extract matching pairs
        pairs = []
        for source in file_dict["sources"]:
            source_url = urlparse(source.strip())
            validate_url(source_url)
            for destination in file_dict["destinations"]:
                dest_url = urlparse(destination.strip())
                validate_url(dest_url)
                pairs.append((source_url, dest_url))

        # Create one File entry per matching pair
        if self.is_bringonline:
            initial_file_state = "STAGING"
        elif self.is_qos_cdmi_transfer:
            initial_file_state = "QOS_TRANSITION"
        else:
            initial_file_state = "SUBMITTED"

        # Multiple replica job or multihop? Then, the initial state is NOT_USED
        if len(file_dict["sources"]) > 1 or self.params["multihop"]:
            # if self.is_bringonline:
            # set the first as STAGING and the rest as 'NOT_USED'
            # staging_and_multihop = True
            # raise HTTPBadRequest('Staging with multiple replicas is not allowed')
            # On multiple replica job, we mark all files initially with NOT_USED
            initial_file_state = "NOT_USED"
            # Multiple replicas, all must share the hashed-id
            if shared_hashed_id is None:
                shared_hashed_id = generate_hashed_id()
        vo_name = self.user.vos[0]

        for source, destination in pairs:
            if len(file_dict["sources"]) > 1 or not is_dest_surl_uuid_enabled(vo_name):
                dest_uuid = None
            else:
                dest_uuid = str(
                    uuid.uuid5(BASE_ID, destination.geturl().encode("utf-8"))
                )
            if self.is_bringonline:
                # add the new query parameter only for root -> EOS-CTA for now
                if source.scheme == "root":
                    if source == destination:
                        destination = self._set_activity_query_string(
                            destination, file_dict
                        )
                    source = self._set_activity_query_string(source, file_dict)

            f = dict(
                job_id=self.job_id,
                file_index=f_index,
                dest_surl_uuid=dest_uuid,
                file_state=initial_file_state,
                source_surl=source.geturl(),
                dest_surl=destination.geturl(),
                source_se=get_storage_element(source),
                dest_se=get_storage_element(destination),
                vo_name=None,
                priority=self.job["priority"],
                user_filesize=safe_filesize(file_dict.get("filesize", 0)),
                selection_strategy=file_dict.get("selection_strategy", "auto"),
                checksum=file_dict.get("checksum", None),
                file_metadata=file_dict.get("metadata", None),
                staging_metadata=file_dict.get("staging_metadata", None),
                archive_metadata=file_dict.get("archive_metadata", None),
                activity=file_dict.get("activity", "default"),
                hashed_id=shared_hashed_id
                if shared_hashed_id
                else generate_hashed_id(),
            )
            if f["file_metadata"] is not None:
                f["file_metadata"] = metadata(f["file_metadata"])
            if f["staging_metadata"] is not None:
                f["staging_metadata"] = metadata(
                    f["staging_metadata"],
                    require_dict=True,
                    name_hint="Staging metadata",
                )
            if f["archive_metadata"] is not None:
                f["archive_metadata"] = metadata(
                    f["archive_metadata"],
                    require_dict=True,
                    name_hint="Archive metadata",
                )
            self.files.append(f)

    def _apply_selection_strategy(self):
        """
        On multiple-replica jobs, select the adequate file to go active
        """
        entry_state = "STAGING" if self.is_bringonline else "SUBMITTED"
        select_best_replica(
            self.files,
            self.user.vos[0],
            entry_state,
            self.files[0].get("selection_strategy", "auto"),
        )

    def _check_checksum(self):
        # If a checksum is provided, but no checksum is available, 'target' comparison
        # (Not nice, but need to keep functionality!) Also by default all files will have ADLER32 checksum type
        has_checksum = False
        for file_dict in self.files:
            if file_dict["checksum"] is not None:
                has_checksum = len(file_dict["checksum"]) > 0
            else:
                file_dict["checksum"] = "ADLER32"

        if type(self.job["checksum_method"]) == bool:
            if not self.job["checksum_method"] and has_checksum:
                self.job["checksum_method"] = "target"
            else:
                if not self.job["checksum_method"] and not has_checksum:
                    self.job["checksum_method"] = "none"
                else:
                    self.job["checksum_method"] = "both"

        self.job["checksum_method"] = self.job["checksum_method"][0]

    def _apply_auto_session_reuse(self):
        # Return early if job type is already "Session Reuse"
        if self.job["job_type"] == "Y":
            return False

        auto_session_reuse = app.config.get("fts3.AutoSessionReuse", False)
        log.debug(
            "job_type={} AutoSessionReuse={}".format(
                self.job["job_type"], auto_session_reuse
            )
        )

        min_reuse_files = int(app.config.get("fts3.AutoSessionReuseMinFiles", 5))
        max_reuse_files = int(app.config.get("fts3.AutoSessionReuseMaxFiles", 1000))
        max_reuse_big_files = int(app.config.get("fts3.AutoSessionReuseMaxBigFiles", 2))
        max_size_small_file = int(
            app.config.get("fts3.AutoSessionReuseMaxSmallFileSize", 104857600)
        )  # 100MB
        max_size_big_file = int(
            app.config.get("fts3.AutoSessionReuseMaxBigFileSize", 1073741824)
        )  # 1GB

        auto_session_reuse_preconditions = (
            auto_session_reuse
            and not self.is_multiple_replica
            and not self.is_bringonline
            and self.job["source_se"]
            and self.job["dest_se"]
            and 1 <= min_reuse_files <= len(self.files) <= max_reuse_files
        )

        if auto_session_reuse and not auto_session_reuse_preconditions:
            log.debug("Skipping AutoSessionReuse: Preconditions not met")
            return False

        if auto_session_reuse_preconditions:
            small_files = 0
            big_files = 0

            for file in self.files:
                if 0 < file["user_filesize"] <= max_size_small_file:
                    small_files += 1
                elif max_size_small_file < file["user_filesize"] <= max_size_big_file:
                    big_files += 1

            if small_files + big_files < len(self.files):
                log.debug(
                    "Skipping AutoSessionReuse: small_files={} + big_files={} < total_files={}".format(
                        small_files, big_files, len(self.files)
                    )
                )
            elif big_files > max_reuse_big_files:
                log.debug(
                    "Skipping AutoSessionReuse: big_files={} > AutoSessionReuseMaxBigFiles={}".format(
                        big_files, max_reuse_big_files
                    )
                )
            else:
                self.job["job_type"] = "Y"
                log.info(
                    "AutoSessionReuse applied: small_files={} big_files={} / total_files={}".format(
                        small_files, big_files, len(self.files)
                    )
                )
                # Reset the hashed IDs to ensure they land on the same machine
                shared_hashed_id = generate_hashed_id()
                for file in self.files:
                    file["hashed_id"] = shared_hashed_id
                return True
        return False

    def _validate_job_type_preconditions(self, unique_files):
        if self.is_multiple_replica:
            if self.job["job_type"] in ("H", "Y"):
                raise BadRequest(
                    "Can not request multiple replica job together with multihop or session reuse at the same time"
                )
            if unique_files > 1:
                raise BadRequest("Multiple replica jobs must only have one unique file")
        if self.job["job_type"] == "Y" and (
            not self.job["source_se"] or not self.job["dest_se"]
        ):
            raise BadRequest(
                "Reuse jobs must only contain transfers for the same source ad destination storage"
            )

    def _populate_transfers(self, files_list):
        """
        Initializes the list of transfers
        """

        job_type = "N"
        if self.params["multihop"]:
            job_type = "H"
        elif safe_flag(self.params["reuse"]):
            job_type = "Y"

        self.is_bringonline = (
            safe_int(self.params["copy_pin_lifetime"]) > 0
            or safe_int(self.params["bring_online"]) > 0
        )

        self.is_qos_cdmi_transfer = (
            self.params["target_qos"] if "target_qos" in self.params.keys() else None
        ) is not None

        if self.is_bringonline:
            job_initial_state = "STAGING"
        elif self.is_qos_cdmi_transfer:
            job_initial_state = "QOS_TRANSITION"
        else:
            job_initial_state = "SUBMITTED"

        max_time_in_queue = seconds_from_value(
            self.params.get("max_time_in_queue", None)
        )
        expiration_time = None
        if max_time_in_queue is not None:
            expiration_time = time.time() + max_time_in_queue

        if max_time_in_queue is not None and safe_int(self.params["bring_online"]) > 0:
            # Ensure that the bringonline and expiration delta is respected
            timeout_delta = seconds_from_value(
                app.config.get("fts3.BringOnlineAndExpirationDelta", None)
            )
            if timeout_delta is not None:
                log.debug(
                    "Will enforce BringOnlineAndExpirationDelta="
                    + str(timeout_delta)
                    + "s"
                )
                if (
                    max_time_in_queue - safe_int(self.params["bring_online"])
                    < timeout_delta
                ):
                    raise BadRequest(
                        "Bringonline and Expiration timeout must be at least "
                        + str(timeout_delta)
                        + " seconds apart"
                    )

        if self.params["overwrite"]:
            overwrite_flag = "Y"
        elif self.params["overwrite_on_retry"]:
            overwrite_flag = "R"
        elif self.params["overwrite_hop"]:
            overwrite_flag = "M"
        else:
            overwrite_flag = None

        self.job = dict(
            job_id=self.job_id,
            job_state=job_initial_state,
            job_type=job_type,
            retry=int(self.params["retry"]),
            retry_delay=int(self.params["retry_delay"]),
            job_params=self.params["gridftp"],
            submit_host=socket.getfqdn(),
            user_dn=None,
            voms_cred=None,
            vo_name=None,
            cred_id=None,
            submit_time=datetime.utcnow(),
            priority=max(min(int(self.params["priority"]), 5), 1),
            space_token=self.params["spacetoken"],
            overwrite_flag=overwrite_flag,
            dst_file_report=safe_flag(self.params["dst_file_report"]),
            source_space_token=self.params["source_spacetoken"],
            copy_pin_lifetime=safe_int(self.params["copy_pin_lifetime"]),
            checksum_method=self.params["verify_checksum"],
            bring_online=safe_int(self.params["bring_online"]),
            archive_timeout=safe_int(self.params["archive_timeout"]),
            job_metadata=self.params["job_metadata"],
            internal_job_params=self._build_internal_job_params(),
            max_time_in_queue=expiration_time,
            target_qos=self.params["target_qos"]
            if "target_qos" in self.params.keys()
            else None,
        )

        if "credential" in self.params:
            self.job["user_cred"] = self.params["credential"]
        elif "credentials" in self.params:
            self.job["user_cred"] = self.params["credentials"]

        # Generate one single "hash" for all files for multihop, session reuse and bringonline jobs
        # (One single hash guarantees transfers are picked up by the same staging/transfer node)
        if self.job["job_type"] in ("H", "Y") or self.is_bringonline:
            shared_hashed_id = generate_hashed_id()
        else:
            shared_hashed_id = None

        # Files
        f_index = 0
        for file_dict in files_list:
            self._populate_files(file_dict, f_index, shared_hashed_id)
            f_index += 1

        if len(self.files) == 0:
            raise BadRequest("No valid pairs available")

        self._check_checksum()
        self._set_job_source_and_destination(self.files)
        self.is_multiple_replica, unique_files = has_multiple_options(self.files)
        self._validate_job_type_preconditions(unique_files)

        if self.is_multiple_replica:
            self.job["job_type"] = "R"
            # Apply selection strategy
            self._apply_selection_strategy()
        # For multihop + staging mark the first as STAGING
        elif self.params["multihop"] and self.is_bringonline:
            self.files[0]["file_state"] = "STAGING"
        # For multihop, mark the first as SUBMITTED
        elif self.params["multihop"]:
            self.files[0]["file_state"] = "SUBMITTED"

        auto_session_reuse_applied = self._apply_auto_session_reuse()
        log.debug(
            "job_type={} reuse={} AutoSessionReuse_applied={}".format(
                self.job["job_type"], self.params["reuse"], auto_session_reuse_applied
            )
        )

    def _populate_deletion(self, deletion_dict):
        """
        Initializes the list of deletions
        """
        self.job = dict(
            job_id=self.job_id,
            job_state="DELETE",
            job_type=None,
            retry=int(self.params["retry"]),
            retry_delay=int(self.params["retry_delay"]),
            job_params=self.params["gridftp"],
            submit_host=socket.getfqdn(),
            user_dn=None,
            voms_cred=None,
            vo_name=None,
            cred_id=None,
            submit_time=datetime.utcnow(),
            priority=3,
            space_token=self.params["spacetoken"],
            overwrite_flag="N",
            dst_file_report="N",
            source_space_token=self.params["source_spacetoken"],
            copy_pin_lifetime=-1,
            checksum_method=None,
            bring_online=None,
            archive_timeout=None,
            job_metadata=self.params["job_metadata"],
            internal_job_params=None,
            max_time_in_queue=self.params["max_time_in_queue"],
        )

        if "credential" in self.params:
            self.job["user_cred"] = self.params["credential"]
        elif "credentials" in self.params:
            self.job["user_cred"] = self.params["credentials"]

        shared_hashed_id = generate_hashed_id()

        # Avoid surl duplication
        unique_surls = []

        for dm in deletion_dict:
            if isinstance(dm, dict):
                entry = dm
            elif isinstance(dm, str):
                entry = dict(surl=dm)
            else:
                raise ValueError("Invalid type for the deletion item (%s)" % type(dm))

            surl = urlparse(entry["surl"])
            validate_url(surl)

            if surl not in unique_surls:
                self.datamanagement.append(
                    dict(
                        job_id=self.job_id,
                        vo_name=None,
                        file_state="DELETE",
                        source_surl=entry["surl"],
                        source_se=get_storage_element(surl),
                        dest_surl=None,
                        dest_se=None,
                        hashed_id=shared_hashed_id,
                        file_metadata=entry.get("metadata", None),
                    )
                )
                unique_surls.append(surl)

        self._set_job_source_and_destination(self.datamanagement)

    def _set_user(self):
        """
        Set the user that triggered the action
        """
        self.job["user_dn"] = self.user.user_dn
        self.job["cred_id"] = self.user.delegation_id
        self.job["voms_cred"] = " ".join(self.user.voms_cred)
        self.job["vo_name"] = self.params.get("vo_name", self.user.vos[0])
        for file in self.files:
            file["vo_name"] = self.params.get("vo_name", self.user.vos[0])
        for dm in self.datamanagement:
            dm["vo_name"] = self.params.get("vo_name", self.user.vos[0])

    def _add_auth_method_on_job_metadata(self):
        if (
            self.params["job_metadata"] is not None
            and self.params["job_metadata"] != "None"
        ):
            self.params["job_metadata"].update({"auth_method": self.user.method})
        else:
            self.params["job_metadata"] = {"auth_method": self.user.method}

    def __init__(self, user, **kwargs):
        """
        Constructor
        """
        try:
            self.user = user
            # Get the job parameters
            self.params = self._get_params(kwargs.pop("params", dict()))

            # Update auth method used
            self._add_auth_method_on_job_metadata()

            files_list = kwargs.pop("files", None)
            datamg_list = kwargs.pop("delete", None)

            if files_list is not None and datamg_list is not None:
                raise BadRequest(
                    "Simultaneous transfer and namespace operations not supported"
                )
            if files_list is None and datamg_list is None:
                raise BadRequest("No transfers or namespace operations specified")
            id_generator = self.params.get("id_generator", "standard")
            if id_generator == "deterministic":
                log.debug("Deterministic")
                sid = self.params.get("sid", None)
                if sid is not None:
                    log.info("sid: " + sid)
                    vo_id = uuid.uuid5(BASE_ID, self.user.vos[0])
                    self.job_id = str(uuid.uuid5(vo_id, str(sid)))
                else:
                    raise BadRequest("Need sid for deterministic job id generation")
            else:
                self.job_id = str(uuid.uuid1())
            self.files = []
            self.datamanagement = []

            if files_list is not None:
                self._populate_transfers(files_list)
            elif datamg_list is not None:
                self._populate_deletion(datamg_list)

            self._set_user()

            # Reject for SE banning
            # If any SE does not accept submissions, reject the whole job
            # Update wait_timeout and wait_timestamp if WAIT_AS is set
            if self.files:
                apply_banning(self.files)
            if self.datamanagement:
                apply_banning(self.datamanagement)

        except ValueError as ex:
            raise BadRequest("Invalid value within the request: %s" % str(ex))
        except TypeError as ex:
            log.exception("JobBuilder -- printing stacktrace")
            raise BadRequest("Malformed request: %s" % str(ex))
        except KeyError as ex:
            raise BadRequest("Missing parameter: %s" % str(ex))
