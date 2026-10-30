//! What a column of one Flow type needs beyond its `Kind`, derived once per type JSON (`Type::normalize()`) and shared
//! by every column and builder of that type: the Type object, its cast lane, its `Physical`, and how its logical
//! values are built.

use std::rc::Rc;

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::Function;
use flow_batch_frame::kind::{self, kind_of, parse_type, Kind, Markup};

use crate::cast::{build_cast_kind, CastKind};
use crate::ctx::{self, call_handle, call_method, ce_method_ref, expect_object, find_class, instance};
use crate::exception::{ext_exception, json_exception};

const PHYSICAL_FOR: &str = "Flow\\ETL\\Values\\Column\\Physical\\PhysicalFor";
const IDENTITY: &str = "Flow\\ETL\\Values\\Column\\Physical\\IdentityPhysical";

pub struct TypePlan {
    pub kind: Kind,
    pub type_zv: Zval,
    pub values: ValueNode,
    pub cast: CastKind,
    /// `$type->cast()` of the concrete Type class.
    pub cast_fn: &'static Function,
    /// `PhysicalFor::physical($type)` and its `toPhysical()`.
    pub physical: Zval,
    pub to_physical: &'static Function,
    /// The physical is `IdentityPhysical`: the cast value is the physical one.
    pub identity: bool,
}

/// `kind::ValueNode` with every markup node holding the `Physical` instance whose `fromPhysical()` builds the DOM value.
pub type ValueNode = kind::ValueNode<Zval>;

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
    let physical = call_method(&instance(PHYSICAL_FOR)?, "physical", &mut [type_zv.shallow_clone()])?;
    let plan = Rc::new(TypePlan {
        kind: kind_of(&parsed).map_err(json_exception)?,
        type_zv: type_zv.shallow_clone(),
        values: kind::value_node(&parsed)
            .map_err(json_exception)?
            .map_markup(&mut |markup| instance(physical_class(markup)))?,
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

fn physical_class(markup: Markup) -> &'static str {
    match markup {
        Markup::XmlDocument => "Flow\\ETL\\Values\\Column\\Physical\\XmlDocumentPhysical",
        Markup::XmlElement => "Flow\\ETL\\Values\\Column\\Physical\\XmlElementPhysical",
        Markup::HtmlDocument => "Flow\\ETL\\Values\\Column\\Physical\\HtmlDocumentPhysical",
        Markup::HtmlElement => "Flow\\ETL\\Values\\Column\\Physical\\HtmlElementPhysical",
    }
}
