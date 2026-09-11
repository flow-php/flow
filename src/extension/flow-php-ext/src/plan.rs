//! Decode plan built from a SCHEMA frame body (a JSON list of normalized definitions):
//! per column a name and a recursive value `Decoder`, in schema order.

use ext_php_rs::exception::PhpException;
use serde::Deserialize;

use crate::ctx::Ctx;
use crate::exception::ext_exception;

pub enum MapKey {
    Integer,
    String,
}

/// Recursive value decoder mirroring `ValueDecoder::decoderFor` dispatch.
pub enum Decoder {
    Integer,
    Float,
    Boolean,
    String,
    Null,
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
    Structure(Vec<(Vec<u8>, Decoder)>),
    Optional(Box<Decoder>),
}

pub struct Column {
    pub name: String,
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
    fields: Vec<StructureElementJson>,
    #[serde(default)]
    allow_extra: bool,
}

#[derive(Deserialize)]
pub struct StructureElementJson {
    // ALWAYS a JSON string - PHP casts the name on normalize(). A JSON number here
    // would fail parse_schema_json for the WHOLE schema, not just this field.
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
    pub nullable: bool,
}

pub fn parse_schema_json(schema_json: &[u8]) -> Result<Vec<NormalizedDefinition>, PhpException> {
    let json = std::str::from_utf8(schema_json)
        .map_err(|_| ext_exception("flow_php failed to decode schema JSON: invalid UTF-8"))?;

    serde_json::from_str(json)
        .map_err(|e| ext_exception(format!("flow_php failed to decode schema JSON: {e}")))
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
                other => {
                    return Err(ext_exception(format!(
                        "flow_php does not support map keys of type \"{other}\""
                    )));
                }
            };

            Decoder::Map(
                key,
                Box::new(build_decoder(
                    type_json.value.as_ref().ok_or_else(|| missing("value"))?,
                )?),
            )
        }
        "structure_v2" => {
            if type_json.fields.is_empty() {
                return Err(ext_exception(
                    "flow_php read a structure type with no fields; the loaded flow_php extension and the \
                     flow-php/etl in use disagree on the structure schema format - reinstall one to match the \
                     other, or set the Floe engine to FloeEngine::php",
                ));
            }

            let mut elements = Vec::with_capacity(type_json.fields.len());

            for field in &type_json.fields {
                elements.push((field.name.clone().into_bytes(), build_decoder(&field.type_)?));
            }

            if type_json.allow_extra {
                return Err(ext_exception(
                    "flow_php does not support structures that allow extra values",
                ));
            }

            Decoder::Structure(elements)
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

pub fn build_plan(schema_json: &[u8], _ctx: &mut Ctx) -> Result<Plan, PhpException> {
    let definitions = parse_schema_json(schema_json)?;

    let mut columns = Vec::with_capacity(definitions.len());

    for definition in &definitions {
        columns.push(Column {
            name: definition.name.clone(),
            decoder: build_decoder(&definition.type_)?,
        });
    }

    Ok(Plan { columns })
}
