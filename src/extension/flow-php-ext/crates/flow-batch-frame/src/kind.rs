//! Flow schema and type JSON (`Schema::normalize()`, `Type::normalize()`) and the column kind a type is stored as.

use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, FieldRef, Fields, TimeUnit};
use serde::Deserialize;

use crate::error::{Error, Part};

#[derive(Deserialize, Default)]
pub struct TypeJson {
    #[serde(rename = "type")]
    pub type_: String,
    element: Option<Box<TypeJson>>,
    key: Option<Box<TypeJson>>,
    value: Option<Box<TypeJson>>,
    base: Option<Box<TypeJson>>,
    #[serde(default)]
    fields: Vec<StructureElementJson>,
    #[serde(default)]
    zone: Option<String>,
    #[serde(default)]
    class: Option<String>,
}

#[derive(Deserialize)]
pub struct StructureElementJson {
    // ALWAYS a JSON string - PHP casts the name on normalize(). A JSON number here
    // would fail parse_schema for the WHOLE schema, not just this field.
    pub name: String,
    #[serde(rename = "type")]
    pub type_: TypeJson,
    #[serde(default)]
    pub optional: bool,
}

impl TypeJson {
    pub fn element(&self) -> Option<&TypeJson> {
        self.element.as_deref()
    }

    pub fn key(&self) -> Option<&TypeJson> {
        self.key.as_deref()
    }

    pub fn value(&self) -> Option<&TypeJson> {
        self.value.as_deref()
    }

    pub fn base(&self) -> Option<&TypeJson> {
        self.base.as_deref()
    }

    pub fn fields(&self) -> &[StructureElementJson] {
        &self.fields
    }

    pub fn zone(&self) -> Option<&[u8]> {
        self.zone.as_deref().map(str::as_bytes)
    }

    pub fn class(&self) -> Option<&[u8]> {
        self.class.as_deref().map(str::as_bytes)
    }

    fn is_optional(&self) -> bool {
        self.type_ == "optional"
    }
}

#[derive(Deserialize)]
pub struct NormalizedDefinition {
    #[serde(rename = "ref")]
    pub name: String,
    #[serde(rename = "type")]
    pub type_: TypeJson,
    pub nullable: bool,
}

pub fn parse_schema(schema_json: &[u8]) -> Result<Vec<NormalizedDefinition>, Error> {
    let json = std::str::from_utf8(schema_json).map_err(|_| Error::SchemaJsonUtf8)?;

    serde_json::from_str(json).map_err(Error::SchemaJson)
}

pub fn parse_type(type_json: &[u8]) -> Result<TypeJson, Error> {
    let json = std::str::from_utf8(type_json).map_err(|_| Error::TypeJsonUtf8)?;

    serde_json::from_str(json).map_err(Error::TypeJson)
}

/// How a column of a Flow type is stored. `Bytes` holds every string-like type (string, non-empty and numeric
/// strings, class strings, json, enum, timezone, xml, xml_element, html, html_element), `Timestamp` a datetime,
/// `Duration` a time and `Int32` a date.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Null,
    Boolean,
    Int64,
    Int32,
    Float64,
    Uuid,
    Bytes,
    Timestamp,
    Duration,
    List(Box<Field>),
    Map(Box<Field>, Box<Field>),
    Struct(Vec<Field>),
}

/// A nested child. `optional` is the structure element's flag for a structure child, and whether the child type is
/// optional for a list element, map key or map value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub kind: Kind,
    pub optional: bool,
}

pub fn kind(type_json: &[u8]) -> Result<Kind, Error> {
    kind_of(&parse_type(type_json)?)
}

pub fn kind_of(parsed: &TypeJson) -> Result<Kind, Error> {
    let missing = |part: Part| Error::MissingPart {
        type_name: parsed.type_.clone(),
        part,
    };
    let child = |name: &str, type_json: &TypeJson| -> Result<Box<Field>, Error> {
        Ok(Box::new(Field {
            name: name.to_owned(),
            kind: kind_of(type_json)?,
            optional: type_json.is_optional(),
        }))
    };

    Ok(match parsed.type_.as_str() {
        "integer" | "positive_integer" => Kind::Int64,
        "float" => Kind::Float64,
        "boolean" => Kind::Boolean,
        "string" | "non_empty_string" | "numeric-string" | "class_string" | "json" | "enum"
        | "timezone" | "xml" | "xml_element" | "html" | "html_element" => Kind::Bytes,
        "datetime" => Kind::Timestamp,
        "date" => Kind::Int32,
        "time" => Kind::Duration,
        "uuid" => Kind::Uuid,
        "null" => Kind::Null,
        "list" => Kind::List(child(
            "item",
            parsed.element().ok_or_else(|| missing(Part::Element))?,
        )?),
        "map" => Kind::Map(
            child("key", parsed.key().ok_or_else(|| missing(Part::Key))?)?,
            child("value", parsed.value().ok_or_else(|| missing(Part::Value))?)?,
        ),
        "structure_v2" => Kind::Struct(
            parsed
                .fields()
                .iter()
                .map(|element| {
                    Ok(Field {
                        name: element.name.clone(),
                        kind: kind_of(&element.type_)?,
                        optional: element.optional,
                    })
                })
                .collect::<Result<_, Error>>()?,
        ),
        "optional" => kind_of(parsed.base().ok_or_else(|| missing(Part::Base))?)?,
        other => {
            return Err(Error::UnknownType {
                type_name: other.to_owned(),
            })
        }
    })
}

pub fn data_type(kind: &Kind) -> DataType {
    match kind {
        Kind::Null => DataType::Null,
        Kind::Boolean => DataType::Boolean,
        Kind::Int64 => DataType::Int64,
        Kind::Int32 => DataType::Date32,
        Kind::Float64 => DataType::Float64,
        Kind::Uuid => DataType::FixedSizeBinary(16),
        Kind::Bytes => DataType::Binary,
        Kind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Kind::Duration => DataType::Duration(TimeUnit::Microsecond),
        Kind::List(element) => DataType::List(field("item", &element.kind)),
        Kind::Map(key, value) => DataType::Map(
            Arc::new(ArrowField::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    ArrowField::new("key", data_type(&key.kind), false),
                    ArrowField::new("value", data_type(&value.kind), true),
                ])),
                false,
            )),
            false,
        ),
        Kind::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|child| field(&child.name, &child.kind))
                .collect(),
        ),
    }
}

/// Nullable: only map entries and keys are not, and data_type() builds those itself.
pub fn field(name: &str, kind: &Kind) -> FieldRef {
    Arc::new(ArrowField::new(name, data_type(kind), true))
}
