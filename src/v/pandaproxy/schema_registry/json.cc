/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/json.h"

#include "json/document.h"
#include "json/schema.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>

#include <absl/container/inlined_vector.h>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rapidjson/error/en.h>

#include <numeric>
#include <string_view>
namespace pandaproxy::schema_registry {

struct json_schema_definition::impl {
    ss::sstring to_json() const {
        json::StringBuffer buf;
        json::Writer<json::StringBuffer> wrt(buf);
        doc.Accept(wrt);
        return {buf.GetString(), buf.GetLength()};
    }

    explicit impl(json::Document doc, std::string_view name)
      : doc{std::move(doc)}
      , name{name} {}

    json::Document doc;
    ss::sstring name;
};

bool operator==(
  const json_schema_definition& lhs, const json_schema_definition& rhs) {
    return lhs.raw() == rhs.raw();
}

std::ostream& operator<<(std::ostream& os, const json_schema_definition& def) {
    fmt::print(
      os,
      "type: {}, definition: {}",
      to_string_view(def.type()),
      def().to_json());
    return os;
}

canonical_schema_definition::raw_string json_schema_definition::raw() const {
    return canonical_schema_definition::raw_string{_impl->to_json()};
}

ss::sstring json_schema_definition::name() const { return {_impl->name}; };

namespace {

// from https://json-schema.org/draft-04/schema, this is used to meta-validate a
// jsonschema
constexpr std::string_view json_draft_4_metaschema = R"json(
{
    "id": "http://json-schema.org/draft-04/schema#",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Core schema meta-schema",
    "definitions": {
        "schemaArray": {
            "type": "array",
            "minItems": 1,
            "items": { "$ref": "#" }
        },
        "positiveInteger": {
            "type": "integer",
            "minimum": 0
        },
        "positiveIntegerDefault0": {
            "allOf": [ { "$ref": "#/definitions/positiveInteger" }, { "default": 0 } ]
        },
        "simpleTypes": {
            "enum": [ "array", "boolean", "integer", "null", "number", "object", "string" ]
        },
        "stringArray": {
            "type": "array",
            "items": { "type": "string" },
            "minItems": 1,
            "uniqueItems": true
        }
    },
    "type": "object",
    "properties": {
        "id": {
            "type": "string"
        },
        "$schema": {
            "type": "string",
            "enum": ["http://json-schema.org/draft-04/schema#"]
        },
        "title": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "default": {},
        "multipleOf": {
            "type": "number",
            "minimum": 0,
            "exclusiveMinimum": true
        },
        "maximum": {
            "type": "number"
        },
        "exclusiveMaximum": {
            "type": "boolean",
            "default": false
        },
        "minimum": {
            "type": "number"
        },
        "exclusiveMinimum": {
            "type": "boolean",
            "default": false
        },
        "maxLength": { "$ref": "#/definitions/positiveInteger" },
        "minLength": { "$ref": "#/definitions/positiveIntegerDefault0" },
        "pattern": {
            "type": "string",
            "format": "regex"
        },
        "additionalItems": {
            "anyOf": [
                { "type": "boolean" },
                { "$ref": "#" }
            ],
            "default": {}
        },
        "items": {
            "anyOf": [
                { "$ref": "#" },
                { "$ref": "#/definitions/schemaArray" }
            ],
            "default": {}
        },
        "maxItems": { "$ref": "#/definitions/positiveInteger" },
        "minItems": { "$ref": "#/definitions/positiveIntegerDefault0" },
        "uniqueItems": {
            "type": "boolean",
            "default": false
        },
        "maxProperties": { "$ref": "#/definitions/positiveInteger" },
        "minProperties": { "$ref": "#/definitions/positiveIntegerDefault0" },
        "required": { "$ref": "#/definitions/stringArray" },
        "additionalProperties": {
            "anyOf": [
                { "type": "boolean" },
                { "$ref": "#" }
            ],
            "default": {}
        },
        "definitions": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "properties": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "patternProperties": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "dependencies": {
            "type": "object",
            "additionalProperties": {
                "anyOf": [
                    { "$ref": "#" },
                    { "$ref": "#/definitions/stringArray" }
                ]
            }
        },
        "enum": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": true
        },
        "type": {
            "anyOf": [
                { "$ref": "#/definitions/simpleTypes" },
                {
                    "type": "array",
                    "items": { "$ref": "#/definitions/simpleTypes" },
                    "minItems": 1,
                    "uniqueItems": true
                }
            ]
        },
        "format": { "type": "string" },
        "allOf": { "$ref": "#/definitions/schemaArray" },
        "anyOf": { "$ref": "#/definitions/schemaArray" },
        "oneOf": { "$ref": "#/definitions/schemaArray" },
        "not": { "$ref": "#" }
    },
    "dependencies": {
        "exclusiveMaximum": [ "maximum" ],
        "exclusiveMinimum": [ "minimum" ]
    },
    "default": {}
}
)json";

result<json::Document> parse_json(std::string_view v) {
    // validation pre-step: compile metaschema for json draft
    static const auto metaschema_doc = [] {
        auto metaschema_json = json::Document{};
        metaschema_json.Parse(
          json_draft_4_metaschema.data(), json_draft_4_metaschema.size());
        vassert(
          !metaschema_json.HasParseError(), "Malformed metaschema document");

        return json::SchemaDocument{metaschema_json};
    }();

    // validation of schema: validate it against metaschema
    // first construct a reader that validates the schema against the metaschema
    // while parsing it
    auto schema_stream = rapidjson::MemoryStream{v.data(), v.size()};
    auto validating_reader
      = json::SchemaValidatingReader<rapidjson::MemoryStream>{
        schema_stream, metaschema_doc};

    // then parse schema to json
    auto schema_json = json::Document{};
    schema_json.Populate(validating_reader);

    if (auto parse_res = validating_reader.GetParseResult();
        parse_res.IsError()) {
        // schema_json is either not a json document
        // or it's not a valid json according to metaschema

        // Check the validation result
        if (!validating_reader.IsValid()) {
            // not a valid schema draft4 according to metaschema. retrieve some
            // info and return error
            auto error_loc_metaschema = json::StringBuffer{};
            auto error_loc_schema = json::StringBuffer{};
            validating_reader.GetInvalidSchemaPointer().StringifyUriFragment(
              error_loc_metaschema);
            validating_reader.GetInvalidDocumentPointer().StringifyUriFragment(
              error_loc_schema);
            auto invalid_keyword = validating_reader.GetInvalidSchemaKeyword();

            return error_info{
              error_code::schema_invalid,
              fmt::format(
                "Invalid json schema: '{}', invalid metaschema: '{}', invalid "
                "keyword: '{}'",
                std::string_view{
                  error_loc_schema.GetString(), error_loc_schema.GetLength()},
                std::string_view{
                  error_loc_metaschema.GetString(),
                  error_loc_metaschema.GetLength()},
                invalid_keyword)};
        } else {
            // not a valid json document, return error
            return error_info{
              error_code::schema_invalid,
              fmt::format(
                "Malformed json schema: {} at offset {}",
                rapidjson::GetParseError_En(parse_res.Code()),
                parse_res.Offset())};
        }
    }

    // schema_json is a valid json and a syntactically valid json schema draft4.
    // TODO AB cross validate "$ref" fields, this is not done automatically

    return {std::move(schema_json)};
}

/// is_superset section

// close the implementation in a namespace to keep it contained
namespace is_superset_impl {

enum class json_type : uint8_t {
    string = 0,
    integer = 1,
    number = 2,
    object = 3,
    array = 4,
    boolean = 5,
    null = 6
};
// enough inlined space to hold all the values of json_type
using json_type_list = absl::InlinedVector<json_type, 7>;

constexpr std::string_view to_string_view(json_type t) {
    switch (t) {
    case json_type::string:
        return "string";
    case json_type::integer:
        return "integer";
    case json_type::number:
        return "number";
    case json_type::object:
        return "object";
    case json_type::array:
        return "array";
    case json_type::boolean:
        return "boolean";
    case json_type::null:
        return "null";
    }
}

constexpr std::optional<json_type> from_string_view(std::string_view v) {
    return string_switch<std::optional<json_type>>(v)
      .match(to_string_view(json_type::string), json_type::string)
      .match(to_string_view(json_type::integer), json_type::integer)
      .match(to_string_view(json_type::number), json_type::number)
      .match(to_string_view(json_type::object), json_type::object)
      .match(to_string_view(json_type::array), json_type::array)
      .match(to_string_view(json_type::boolean), json_type::boolean)
      .match(to_string_view(json_type::null), json_type::null)
      .default_match(std::nullopt);
}

constexpr auto parse_json_type(json::Value const& v) {
    std::string_view sv{v.GetString(), v.GetStringLength()};
    auto type = from_string_view(sv);
    if (!type) {
        throw as_exception(error_info{
          error_code::schema_invalid,
          fmt::format("Invalid JSON Schema type: '{}'", sv)});
    }
    return *type;
}

// parse None | schema_type | array[schema_type] into a set of types.
// the return type is implemented as a inlined_vector<json_type> with sorted set
// semantics
json_type_list normalized_type(json::Value const& v) {
    auto type_it = v.FindMember("type");
    auto ret = json_type_list{};
    if (type_it == v.MemberEnd()) {
        // omit keyword is like accepting all the types
        ret = {
          json_type::string,
          json_type::integer,
          json_type::number,
          json_type::object,
          json_type::array,
          json_type::boolean,
          json_type::null};
    } else if (type_it->value.IsArray()) {
        // schema ensures that all the values are unique
        for (auto& v : type_it->value.GetArray()) {
            ret.push_back(parse_json_type(v));
        }
    } else {
        ret.push_back(parse_json_type(type_it->value));
    }

    // to support set difference operations, sort the elements
    std::ranges::sort(ret);
    return ret;
}

} // namespace is_superset_impl
} // namespace

ss::future<json_schema_definition>
make_json_schema_definition(sharded_store&, canonical_schema schema) {
    auto doc = parse_json(schema.def().raw()()).value(); // throws on error
    std::string_view name = schema.sub()();
    auto refs = std::move(schema).def().refs();
    co_return json_schema_definition{
      ss::make_shared<json_schema_definition::impl>(std::move(doc), name),
      std::move(refs)};
}

ss::future<canonical_schema>
make_canonical_json_schema(sharded_store&, unparsed_schema def) {
    // TODO BP: More validation and normalisation
    parse_json(def.def().raw()()).value(); // throws on error
    co_return canonical_schema{
      def.sub(), canonical_schema_definition{def.def().raw(), def.type()}};
}

} // namespace pandaproxy::schema_registry
