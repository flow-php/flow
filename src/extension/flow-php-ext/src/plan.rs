//! Decode plan built from a SCHEMA frame body via the canonical PHP `SchemaDecoder`.

use std::fmt;

use ext_php_rs::convert::IntoZvalDyn;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::ClassEntry;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::ctx::{ensure_no_pending_exception, find_class, property_offset, read_property, Ctx};
use crate::exception::ext_exception;

/// Mirror of `SchemaDecoder::ENTRY_CLASSES`; dual-engine tests break loudly
/// when either side changes.
const DEFINITION_TO_ENTRY: [(&str, &str); 17] = [
    (
        "Flow\\ETL\\Schema\\Definition\\BooleanDefinition",
        "Flow\\ETL\\Row\\Entry\\BooleanEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\DateDefinition",
        "Flow\\ETL\\Row\\Entry\\DateEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\DateTimeDefinition",
        "Flow\\ETL\\Row\\Entry\\DateTimeEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\EnumDefinition",
        "Flow\\ETL\\Row\\Entry\\EnumEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\FloatDefinition",
        "Flow\\ETL\\Row\\Entry\\FloatEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\HTMLDefinition",
        "Flow\\ETL\\Row\\Entry\\HTMLEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\HTMLElementDefinition",
        "Flow\\ETL\\Row\\Entry\\HTMLElementEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\IntegerDefinition",
        "Flow\\ETL\\Row\\Entry\\IntegerEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\JsonDefinition",
        "Flow\\ETL\\Row\\Entry\\JsonEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\ListDefinition",
        "Flow\\ETL\\Row\\Entry\\ListEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\MapDefinition",
        "Flow\\ETL\\Row\\Entry\\MapEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\StringDefinition",
        "Flow\\ETL\\Row\\Entry\\StringEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\StructureDefinition",
        "Flow\\ETL\\Row\\Entry\\StructureEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\TimeDefinition",
        "Flow\\ETL\\Row\\Entry\\TimeEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\UuidDefinition",
        "Flow\\ETL\\Row\\Entry\\UuidEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\XMLDefinition",
        "Flow\\ETL\\Row\\Entry\\XMLEntry",
    ),
    (
        "Flow\\ETL\\Schema\\Definition\\XMLElementDefinition",
        "Flow\\ETL\\Row\\Entry\\XMLElementEntry",
    ),
];

pub enum MapKey {
    Integer,
    String,
    Dynamic,
}

/// Recursive value decoder mirroring `ValueDecoder::decoderFor` dispatch.
pub enum Decoder {
    Integer,
    Float,
    Boolean,
    String,
    Null,
    Dynamic,
    DateTime,
    Interval,
    Uuid,
    Json,
    TimeZone,
    Enum,
    Xml,
    XmlElement,
    Html,
    HtmlElement,
    List(Box<Decoder>),
    Map(MapKey, Box<Decoder>),
    Structure(Vec<(Vec<u8>, Decoder)>, bool),
    Optional(Box<Decoder>),
}

pub struct Column {
    pub name: String,
    pub name_zv: Zval,
    pub entry_ce: &'static ClassEntry,
    pub name_slot: u32,
    pub value_slot: u32,
    pub definition_slot: u32,
    pub definition: Zval,
    pub nullable_definition: Zval,
    pub from_null_definition: Zval,
    pub decoder: Decoder,
}

pub struct Plan {
    pub columns: Vec<Column>,
}

#[derive(Deserialize, Default)]
pub struct TypeJson {
    #[serde(rename = "type")]
    pub type_: String,
    element: Option<Box<TypeJson>>,
    key: Option<Box<TypeJson>>,
    value: Option<Box<TypeJson>>,
    base: Option<Box<TypeJson>>,
    #[serde(default)]
    elements: OrderedTypes,
    #[serde(default)]
    optional_elements: OrderedTypes,
    #[serde(default)]
    allow_extra: bool,
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

    pub fn all_elements(&self) -> impl Iterator<Item = (&String, &TypeJson)> {
        self.elements
            .0
            .iter()
            .chain(&self.optional_elements.0)
            .map(|(name, element)| (name, element))
    }

    pub fn allow_extra(&self) -> bool {
        self.allow_extra
    }
}

#[derive(Deserialize)]
pub struct NormalizedDefinition {
    #[serde(rename = "ref")]
    pub name: String,
    #[serde(rename = "type")]
    pub type_: TypeJson,
}

pub fn parse_schema_json(schema_json: &[u8]) -> Result<Vec<NormalizedDefinition>, PhpException> {
    let json = std::str::from_utf8(schema_json)
        .map_err(|_| ext_exception("flow_php failed to decode schema JSON: invalid UTF-8"))?;

    serde_json::from_str(json)
        .map_err(|e| ext_exception(format!("flow_php failed to decode schema JSON: {e}")))
}

#[derive(Default)]
struct OrderedTypes(Vec<(String, TypeJson)>);

impl<'de> Deserialize<'de> for OrderedTypes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OrderedTypesVisitor;

        impl<'de> Visitor<'de> for OrderedTypesVisitor {
            type Value = OrderedTypes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of element names to types")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut entries = Vec::with_capacity(access.size_hint().unwrap_or(0));

                while let Some((key, value)) = access.next_entry::<String, TypeJson>()? {
                    entries.push((key, value));
                }

                Ok(OrderedTypes(entries))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                _: A,
            ) -> Result<Self::Value, A::Error> {
                // an empty PHP array json_encodes as [] instead of {}
                Ok(OrderedTypes(Vec::new()))
            }
        }

        deserializer.deserialize_any(OrderedTypesVisitor)
    }
}

fn build_decoder(type_json: &TypeJson) -> Result<Decoder, PhpException> {
    let missing = |part: &str| {
        ext_exception(format!(
            "flow_php schema JSON for type \"{}\" is missing its {part}",
            type_json.type_
        ))
    };

    Ok(match type_json.type_.as_str() {
        "integer" | "positive_integer" => Decoder::Integer,
        "float" => Decoder::Float,
        "boolean" => Decoder::Boolean,
        "string" | "non_empty_string" | "numeric-string" | "class_string" => Decoder::String,
        "null" => Decoder::Null,
        "mixed" | "union" | "scalar" | "literal" | "array" => Decoder::Dynamic,
        "datetime" | "date" => Decoder::DateTime,
        "time" => Decoder::Interval,
        "uuid" => Decoder::Uuid,
        "json" => Decoder::Json,
        "timezone" => Decoder::TimeZone,
        "enum" => Decoder::Enum,
        "xml" => Decoder::Xml,
        "xml_element" => Decoder::XmlElement,
        "html" => Decoder::Html,
        "html_element" => Decoder::HtmlElement,
        "list" => Decoder::List(Box::new(build_decoder(
            type_json
                .element
                .as_ref()
                .ok_or_else(|| missing("element"))?,
        )?)),
        "map" => {
            let key = match type_json
                .key
                .as_ref()
                .ok_or_else(|| missing("key"))?
                .type_
                .as_str()
            {
                "integer" => MapKey::Integer,
                "string" => MapKey::String,
                _ => MapKey::Dynamic,
            };

            Decoder::Map(
                key,
                Box::new(build_decoder(
                    type_json.value.as_ref().ok_or_else(|| missing("value"))?,
                )?),
            )
        }
        "structure" => {
            let mut elements = Vec::with_capacity(
                type_json.elements.0.len() + type_json.optional_elements.0.len(),
            );

            for (name, element) in type_json
                .elements
                .0
                .iter()
                .chain(&type_json.optional_elements.0)
            {
                elements.push((name.clone().into_bytes(), build_decoder(element)?));
            }

            Decoder::Structure(elements, type_json.allow_extra)
        }
        "optional" => Decoder::Optional(Box::new(build_decoder(
            type_json.base.as_ref().ok_or_else(|| missing("base"))?,
        )?)),
        other => {
            return Err(ext_exception(format!(
                "flow_php does not support values of type \"{other}\" in this build"
            )));
        }
    })
}

fn definition_class_for_type(type_str: &str) -> Option<&'static str> {
    Some(match type_str {
        "integer" | "positive_integer" => "Flow\\ETL\\Schema\\Definition\\IntegerDefinition",
        "float" => "Flow\\ETL\\Schema\\Definition\\FloatDefinition",
        "boolean" => "Flow\\ETL\\Schema\\Definition\\BooleanDefinition",
        "string" | "non_empty_string" | "numeric-string" | "class_string" => {
            "Flow\\ETL\\Schema\\Definition\\StringDefinition"
        }
        "date" => "Flow\\ETL\\Schema\\Definition\\DateDefinition",
        "datetime" => "Flow\\ETL\\Schema\\Definition\\DateTimeDefinition",
        "time" => "Flow\\ETL\\Schema\\Definition\\TimeDefinition",
        "json" | "array" => "Flow\\ETL\\Schema\\Definition\\JsonDefinition",
        "uuid" => "Flow\\ETL\\Schema\\Definition\\UuidDefinition",
        "list" => "Flow\\ETL\\Schema\\Definition\\ListDefinition",
        "map" => "Flow\\ETL\\Schema\\Definition\\MapDefinition",
        "structure" => "Flow\\ETL\\Schema\\Definition\\StructureDefinition",
        "enum" => "Flow\\ETL\\Schema\\Definition\\EnumDefinition",
        "html" => "Flow\\ETL\\Schema\\Definition\\HTMLDefinition",
        "html_element" => "Flow\\ETL\\Schema\\Definition\\HTMLElementDefinition",
        "xml" => "Flow\\ETL\\Schema\\Definition\\XMLDefinition",
        "xml_element" => "Flow\\ETL\\Schema\\Definition\\XMLElementDefinition",
        _ => return None,
    })
}

pub fn entry_class_for_type(type_json: &TypeJson) -> Result<&'static ClassEntry, PhpException> {
    let definition_class =
        definition_class_for_type(type_json.type_.as_str()).ok_or_else(|| {
            ext_exception(format!(
                "flow_php cannot encode a column of type \"{}\"",
                type_json.type_
            ))
        })?;

    let entry_class = DEFINITION_TO_ENTRY
        .iter()
        .find(|(definition_name, _)| *definition_name == definition_class)
        .map(|(_, entry_class)| *entry_class)
        .ok_or_else(|| {
            ext_exception(format!(
                "flow_php cannot encode entries for definition \"{definition_class}\""
            ))
        })?;

    find_class(entry_class)
}

fn entry_class_for(definition: &Zval) -> Result<&'static ClassEntry, PhpException> {
    let definition_class = definition
        .object()
        .and_then(|obj| unsafe { obj.ce.as_ref() })
        .and_then(ClassEntry::name)
        .ok_or_else(|| ext_exception("flow_php failed to resolve a definition class"))?;

    let entry_class = DEFINITION_TO_ENTRY
        .iter()
        .find(|(definition_name, _)| *definition_name == definition_class)
        .map(|(_, entry_class)| *entry_class)
        .ok_or_else(|| {
            ext_exception(format!(
                "flow_php cannot hydrate entries for definition \"{definition_class}\""
            ))
        })?;

    find_class(entry_class)
}

pub fn build_plan(schema_json: &[u8], ctx: &mut Ctx) -> Result<Plan, PhpException> {
    let definitions = parse_schema_json(schema_json)?;

    let json_string = std::str::from_utf8(schema_json)
        .expect("validated by parse_schema_json")
        .to_string();
    let hydrator_columns = ctx
        .schema_decoder()?
        .try_call_method("decode", vec![&json_string as &dyn IntoZvalDyn])
        .map_err(|e| ext_exception(format!("flow_php failed to decode a schema frame: {e:?}")))?;
    ensure_no_pending_exception("decode a schema frame")?;

    let hydrator_columns = hydrator_columns.array().ok_or_else(|| {
        ext_exception("flow_php expected SchemaDecoder::decode to return an array")
    })?;

    if hydrator_columns.len() != definitions.len() {
        return Err(ext_exception(
            "flow_php schema JSON and SchemaDecoder disagree on the column count",
        ));
    }

    let mut columns = Vec::with_capacity(definitions.len());

    for (index, definition) in definitions.iter().enumerate() {
        let hydrator_column = hydrator_columns
            .get_index(index as i64)
            .and_then(Zval::object)
            .ok_or_else(|| ext_exception("flow_php expected a HydratorColumn object"))?;

        let name_zv = read_property(hydrator_column, "name")?;
        let name = name_zv
            .zend_str()
            .and_then(|s| std::str::from_utf8(s.as_bytes()).ok())
            .ok_or_else(|| ext_exception("flow_php expected column names to be UTF-8 strings"))?
            .to_string();

        let read_definition = |property: &str| -> Result<Zval, PhpException> {
            let zv = read_property(hydrator_column, property)?;

            if !zv.is_object() {
                return Err(ext_exception(
                    "flow_php expected HydratorColumn definitions to be objects",
                ));
            }

            Ok(zv)
        };

        let definition_zv = read_definition("definition")?;
        let entry_ce = entry_class_for(&definition_zv)?;

        columns.push(Column {
            name,
            name_zv,
            entry_ce,
            name_slot: property_offset(entry_ce, "name")?,
            value_slot: property_offset(entry_ce, "value")?,
            definition_slot: property_offset(entry_ce, "definition")?,
            definition: definition_zv,
            nullable_definition: read_definition("nullableDefinition")?,
            from_null_definition: read_definition("fromNullDefinition")?,
            decoder: build_decoder(&definition.type_)?,
        });
    }

    Ok(Plan { columns })
}
