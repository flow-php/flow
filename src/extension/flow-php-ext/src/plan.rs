//! What a column of one Flow type needs beyond its `Kind`, derived once per type JSON (`Type::normalize()`) and shared
//! by every column and builder of that type: the Type object, its cast lane, its `Physical`, and how its logical
//! values are built.

use std::rc::Rc;

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::Function;
use flow_batch_frame::kind::{kind_of, parse_type, Kind, TypeJson};

use crate::cast::{build_cast_kind, CastKind};
use crate::ctx::{self, call_handle, call_method, ce_method_ref, expect_object, find_class, instance};
use crate::exception::{ext_exception, json_exception};

const PHYSICAL_FOR: &str = "Flow\\ETL\\Column\\Physical\\PhysicalFor";
const IDENTITY: &str = "Flow\\ETL\\Column\\Physical\\IdentityPhysical";

pub struct TypePlan {
    pub kind: Kind,
    pub type_zv: Zval,
    pub values: ValueNode,
    pub cast: CastKind,
    /// `$type->cast()` of the concrete Type class.
    pub cast_fn: &'static Function,
    /// `PhysicalFor::type($type)` and its `toPhysical()`.
    pub physical: Zval,
    pub to_physical: &'static Function,
    /// The physical is `IdentityPhysical`: the cast value is the physical one.
    pub identity: bool,
}

/// How a logical value is built from its physical one.
pub enum ValueNode {
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
    Markup(Zval),
    List(Box<ValueNode>),
    Map(Box<ValueNode>),
    Struct(Vec<ValueNode>),
}

/// The plan of `$type`, built on first use of its type JSON.
pub fn type_plan(type_zv: &Zval) -> Result<Rc<TypePlan>, PhpException> {
    if let Some(plan) = ctx::type_plan_of(type_zv)? {
        return Ok(plan);
    }

    let plan = json_plan(type_zv)?;
    ctx::store_type_plan(type_zv, &plan)?;

    Ok(plan)
}

fn json_plan(type_zv: &Zval) -> Result<Rc<TypePlan>, PhpException> {
    let normalized = call_method(type_zv, "normalize", &mut [])?;
    let json = call_handle(ctx::json_encode()?, None, &mut [normalized], "encode a type as JSON")?;
    let json = json
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected type JSON to be a string"))?
        .as_bytes()
        .to_vec();

    if let Some(plan) = ctx::plan(&json)? {
        return Ok(plan);
    }

    let parsed = parse_type(&json).map_err(json_exception)?;
    let type_obj = expect_object(type_zv, "a Type")?;
    let type_ce =
        unsafe { type_obj.ce.as_ref() }.ok_or_else(|| ext_exception("flow_php failed to resolve a Type class"))?;
    let physical = call_method(&instance(PHYSICAL_FOR)?, "type", &mut [type_zv.shallow_clone()])?;
    let plan = Rc::new(TypePlan {
        kind: kind_of(&parsed).map_err(json_exception)?,
        type_zv: type_zv.shallow_clone(),
        values: value_node(&parsed)?,
        cast: build_cast_kind(&parsed),
        cast_fn: ce_method_ref(type_ce, "cast")?,
        to_physical: ce_method_ref(
            unsafe { expect_object(&physical, "a Physical")?.ce.as_ref() }
                .ok_or_else(|| ext_exception("flow_php failed to resolve a Physical class"))?,
            "toPhysical",
        )?,
        physical: physical.shallow_clone(),
        identity: expect_object(&physical, "a Physical")?.instance_of(find_class(IDENTITY)?),
    });
    ctx::store_plan(json, &plan)?;

    Ok(plan)
}

/// `PhysicalFor::definition($definition)`: its refusals (wildcard enum, mixed, a foreign definition) are PHP's.
pub fn physical_for_definition(definition: &Zval) -> Result<Zval, PhpException> {
    call_method(
        &instance(PHYSICAL_FOR)?,
        "definition",
        &mut [definition.shallow_clone()],
    )
}

fn value_node(parsed: &TypeJson) -> Result<ValueNode, PhpException> {
    let child = |part: Option<&TypeJson>| -> Result<Box<ValueNode>, PhpException> {
        Ok(Box::new(value_node(part.ok_or_else(|| {
            ext_exception("flow_php type JSON is missing a nested type")
        })?)?))
    };

    Ok(match parsed.type_.as_str() {
        "datetime" => ValueNode::DateTime(parsed.zone().unwrap_or(b"UTC").to_vec()),
        "date" => ValueNode::Date,
        "time" => ValueNode::Time,
        "uuid" => ValueNode::Uuid,
        "json" => ValueNode::Json,
        "enum" => ValueNode::Enum(
            parsed
                .class()
                .ok_or_else(|| ext_exception("flow_php enum type JSON is missing its class"))?
                .to_vec(),
        ),
        "timezone" => ValueNode::TimeZone,
        "xml" => ValueNode::Markup(instance("Flow\\ETL\\Column\\Physical\\XmlDocumentPhysical")?),
        "xml_element" => ValueNode::Markup(instance("Flow\\ETL\\Column\\Physical\\XmlElementPhysical")?),
        "html" => ValueNode::Markup(instance("Flow\\ETL\\Column\\Physical\\HtmlDocumentPhysical")?),
        "html_element" => ValueNode::Markup(instance("Flow\\ETL\\Column\\Physical\\HtmlElementPhysical")?),
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
