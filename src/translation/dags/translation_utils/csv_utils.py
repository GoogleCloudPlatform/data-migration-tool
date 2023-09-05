# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging


def row_to_dict(headers, row):
    return {headers[i]: row[i] for i in range(len(headers))}


def pattern_filter(pattern):
    """
    creates a pattern filter for each field
    pattern should be of the form FIELD1:KEYWORD1,FIELD2:KEYWORD1,...
    this will filter rows that contains KEYWORD1 on FIELD1 or KEYWORD2 on FIELD2, etc...
    """
    if not pattern or pattern == "":
        return None

    field_patterns = list(map(lambda f: f.strip().split("="), pattern.split(",")))

    def filter(row):
        for [field, keyword] in field_patterns:
            if field in row and row[field].find(keyword) >= 0:
                return True

        return False

    return filter


matchers = {
    "equals": lambda str, value: str == value,
    "contains": lambda str, value: str.find(value) >= 0,
}


def rules_filter(rules):
    """
    creates an error row filter based on
    rules specs
    """

    if not isinstance(rules, list) or not rules:
        logging.info("returning None error row filter. no rules supplied")
        return None

    logging.info(f"creating error row filter for rules: {rules}")

    def filter(row):
        for rule in rules:
            field = rule["field"]
            matchType = rule["matchType"] if "matchType" in rule else "equals"
            caseSensitive = rule["caseSensitive"] if "caseSensitive" in rule else False
            value = rule["value"]
            fieldValue = row[field]

            if not caseSensitive:
                value = value.casefold()
                fieldValue = fieldValue.casefold()

            if field in row and matchers[matchType](fieldValue, value):
                return True

        return False

    return filter
