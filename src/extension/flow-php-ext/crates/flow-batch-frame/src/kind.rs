//! Flow schema and type JSON (`Schema::normalize()`, `Type::normalize()`), the column kind a type is stored as and the
//! shape its logical values are built in.

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
    // always a JSON string, because PHP casts the name
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

/// How a column of a Flow type is stored.
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

/// A nested child; `optional` is the element's flag, or the child type's optionality inside a list or map.
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
        "string" | "non_empty_string" | "numeric-string" | "class_string" | "json" | "enum" | "timezone" | "xml"
        | "xml_element" | "html" | "html_element" => Kind::Bytes,
        "datetime" => Kind::Timestamp,
        "date" => Kind::Int32,
        "time" => Kind::Duration,
        "uuid" => Kind::Uuid,
        "null" => Kind::Null,
        "list" => Kind::List(child("item", parsed.element().ok_or_else(|| missing(Part::Element))?)?),
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

// Must equal arrow-ext's parquet/canonical.rs canonical_type(), nested names and nullability included - pinned by phpt 091.
pub fn data_type(kind: &Kind) -> DataType {
    match kind {
        Kind::Null => DataType::Null,
        Kind::Boolean => DataType::Boolean,
        Kind::Int64 => DataType::Int64,
        Kind::Int32 => DataType::Date32,
        Kind::Float64 => DataType::Float64,
        Kind::Uuid => DataType::FixedSizeBinary(16),
        Kind::Bytes => DataType::LargeBinary,
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
        Kind::Struct(fields) => DataType::Struct(fields.iter().map(|child| field(&child.name, &child.kind)).collect()),
    }
}

/// Nullable: only map entries and keys are not, and data_type() builds those itself.
pub fn field(name: &str, kind: &Kind) -> FieldRef {
    Arc::new(ArrowField::new(name, data_type(kind), true))
}

/// How a logical value is built from its physical one; [`Markup`] names the physical class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueNode<M> {
    Plain,
    Null,
    DateTime(Vec<u8>),
    Date,
    Time,
    Uuid,
    Json,
    Enum(Vec<u8>),
    TimeZone,
    /// A markup `Physical` whose `fromPhysical()` builds the DOM value (D4).
    Markup(M),
    List(Box<ValueNode<M>>),
    Map(Box<ValueNode<M>>),
    Struct(Vec<ValueNode<M>>),
}

/// The `Flow\ETL\Column\Physical` class of a markup type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Markup {
    XmlDocument,
    XmlElement,
    HtmlDocument,
    HtmlElement,
}

impl<M> ValueNode<M> {
    /// The same shape with every markup payload replaced by `markup(payload)`.
    pub fn map_markup<N, E>(self, markup: &mut impl FnMut(M) -> Result<N, E>) -> Result<ValueNode<N>, E> {
        Ok(match self {
            ValueNode::Plain => ValueNode::Plain,
            ValueNode::Null => ValueNode::Null,
            ValueNode::DateTime(zone) => ValueNode::DateTime(zone),
            ValueNode::Date => ValueNode::Date,
            ValueNode::Time => ValueNode::Time,
            ValueNode::Uuid => ValueNode::Uuid,
            ValueNode::Json => ValueNode::Json,
            ValueNode::Enum(class) => ValueNode::Enum(class),
            ValueNode::TimeZone => ValueNode::TimeZone,
            ValueNode::Markup(payload) => ValueNode::Markup(markup(payload)?),
            ValueNode::List(element) => ValueNode::List(Box::new(element.map_markup(markup)?)),
            ValueNode::Map(value) => ValueNode::Map(Box::new(value.map_markup(markup)?)),
            ValueNode::Struct(elements) => ValueNode::Struct(
                elements
                    .into_iter()
                    .map(|element| element.map_markup(markup))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

/// The value shape of `parsed`; an optional type has its base's shape.
pub fn value_node(parsed: &TypeJson) -> Result<ValueNode<Markup>, Error> {
    let child = |part: Option<&TypeJson>| -> Result<Box<ValueNode<Markup>>, Error> {
        Ok(Box::new(value_node(part.ok_or(Error::MissingNestedType)?)?))
    };

    Ok(match parsed.type_.as_str() {
        "datetime" => ValueNode::DateTime(parsed.zone().unwrap_or(b"UTC").to_vec()),
        "date" => ValueNode::Date,
        "time" => ValueNode::Time,
        "uuid" => ValueNode::Uuid,
        "json" => ValueNode::Json,
        "enum" => ValueNode::Enum(parsed.class().ok_or(Error::MissingEnumClass)?.to_vec()),
        "timezone" => ValueNode::TimeZone,
        "xml" => ValueNode::Markup(Markup::XmlDocument),
        "xml_element" => ValueNode::Markup(Markup::XmlElement),
        "html" => ValueNode::Markup(Markup::HtmlDocument),
        "html_element" => ValueNode::Markup(Markup::HtmlElement),
        "null" => ValueNode::Null,
        "list" => ValueNode::List(child(parsed.element())?),
        "map" => ValueNode::Map(child(parsed.value())?),
        "structure_v2" => ValueNode::Struct(
            parsed
                .fields()
                .iter()
                .map(|field| value_node(&field.type_))
                .collect::<Result<_, _>>()?,
        ),
        "optional" => *child(parsed.base())?,
        _ => ValueNode::Plain,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_type, value_node, Markup, ValueNode};

    #[test]
    fn a_value_node_follows_the_type_json_and_an_optional_is_its_base() {
        let parsed = parse_type(
            br#"{"type":"structure_v2","fields":[
                {"name":"at","type":{"type":"datetime","zone":"Europe/Warsaw"}},
                {"name":"tags","type":{"type":"list","element":{"type":"optional","base":{"type":"xml"}}}},
                {"name":"n","type":{"type":"integer"}}
            ]}"#,
        )
        .unwrap();

        assert_eq!(
            value_node(&parsed).unwrap(),
            ValueNode::Struct(vec![
                ValueNode::DateTime(b"Europe/Warsaw".to_vec()),
                ValueNode::List(Box::new(ValueNode::Markup(Markup::XmlDocument))),
                ValueNode::Plain,
            ])
        );
    }

    #[test]
    fn map_markup_replaces_every_payload_in_place() {
        let node = ValueNode::Map(Box::new(ValueNode::Struct(vec![
            ValueNode::Markup(Markup::HtmlElement),
            ValueNode::Uuid,
        ])));
        let mapped: ValueNode<&str> = node
            .map_markup(&mut |markup| {
                Ok::<_, ()>(match markup {
                    Markup::HtmlElement => "html element",
                    _ => "other",
                })
            })
            .unwrap();

        assert_eq!(
            mapped,
            ValueNode::Map(Box::new(ValueNode::Struct(vec![
                ValueNode::Markup("html element"),
                ValueNode::Uuid
            ])))
        );
        assert_eq!(
            ValueNode::Markup(Markup::XmlElement).map_markup(&mut |_| Err::<(), _>("refused")),
            Err("refused")
        );
    }
}
