# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import googleapiclient.discovery
from googleapiclient import http

from common_utils import custom_user_agent


def build_from_document_with_custom_http(discovery_url, custom_http):
    http.set_user_agent(custom_http, custom_user_agent.USER_AGENT)
    req = http.HttpRequest(
        http.build_http(), http.HttpRequest.null_postproc, discovery_url
    )
    resp, content = req.execute()
    return googleapiclient.discovery.build_from_document(
        content,
        http=custom_http,
    )
