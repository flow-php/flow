<?php

declare(strict_types=1);

namespace Flow\Types\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\Types\Type;
use Flow\Types\Type\{Comparator, TypeDetector, TypeFactory, Types};
use Flow\Types\Type\Logical\{ClassStringType,
    DateTimeType,
    DateType,
    InstanceOfType,
    JsonType,
    ListType,
    LiteralType,
    MapType,
    NonEmptyStringType,
    NumericStringType,
    OptionalType,
    PositiveIntegerType,
    ScalarType,
    StructureType,
    TimeType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\Types\Type\Native\{ArrayType,
    BooleanType,
    CallableType,
    EnumType,
    FloatType,
    IntegerType,
    IntersectionType,
    MixedType,
    NullType,
    ObjectType,
    ResourceType,
    StringType,
    UnionType};
use Flow\Types\Value\Uuid;
use UnitEnum;

/**
 * @template T
 *
 * @param array<string, Type<T>> $elements
 * @param array<string, Type<T>> $optional_elements
 *
 * @return Type<array<string, T>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_structure(array $elements = [], array $optional_elements = [], bool $allow_extra = false) : Type
{
    return new StructureType($elements, $optional_elements, $allow_extra);
}

/**
 * @template T
 * @template T
 * @template T
 *
 * @param Type<T> $first
 * @param Type<T> $second
 * @param Type<T> ...$types
 *
 * @return Type<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_union(Type $first, Type $second, Type ...$types) : Type
{
    $type = new UnionType($first, $second);

    foreach ($types as $t) {
        $type = new UnionType($type, $t);
    }

    return $type;
}

/**
 * @template T
 *
 * @param Type<T> $first
 * @param Type<T> $second
 * @param Type<T> ...$types
 *
 * @return Type<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_intersection(Type $first, Type $second, Type ...$types) : Type
{
    $type = new IntersectionType($first, $second);

    foreach ($types as $t) {
        $type = new IntersectionType($type, $t);
    }

    return $type;
}

/**
 * @return Type<numeric-string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_numeric_string() : Type
{
    return new NumericStringType();
}

/**
 * @template T
 *
 * @param Type<T> $type
 *
 * @return Type<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_optional(Type $type) : Type
{
    return new OptionalType($type);
}

/**
 * @param array<string, mixed> $data
 *
 * @return Type<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_from_array(array $data) : Type
{
    return TypeFactory::fromArray($data);
}

/**
 * @template T
 *
 * @param Type<T> $type
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_is_nullable(Type $type) : bool
{
    if ($type instanceof OptionalType) {
        return true;
    }

    if ($type instanceof UnionType) {
        foreach ($type->types()->all() as $nextType) {
            if ($nextType instanceof NullType) {
                return true;
            }
        }
    }

    return false;
}

/**
 * @param Type<mixed> $left
 * @param Type<mixed> $right
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_equals(Type $left, Type $right) : bool
{
    return (new Comparator())->equals($left, $right);
}

/**
 * @template T
 *
 * @param Type<T> ...$types
 *
 * @return Types<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function types(Type ...$types) : Types
{
    return new Types(...$types);
}

/**
 * @template T
 *
 * @param Type<T> $element
 *
 * @return ListType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_list(Type $element) : ListType
{
    return new ListType($element);
}

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @param Type<TKey> $key_type
 * @param Type<TValue> $value_type
 *
 * @return Type<array<TKey, TValue>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_map(Type $key_type, Type $value_type) : Type
{
    return new MapType($key_type, $value_type);
}

/**
 * @return Type<string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_json() : Type
{
    return new JsonType();
}

/**
 * @return Type<\DateTimeInterface>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_datetime() : Type
{
    return new DateTimeType();
}

/**
 * @return Type<\DateTimeInterface>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_date() : Type
{
    return new DateType();
}

/**
 * @return Type<\DateInterval>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_time() : Type
{
    return new TimeType();
}

/**
 * @return Type<\DOMDocument>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_xml() : Type
{
    return new XMLType();
}

/**
 * @return Type<\DOMElement>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_xml_element() : Type
{
    return new XMLElementType();
}

/**
 * @return Type<Uuid>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_uuid() : Type
{
    return new UuidType();
}

/**
 * @return Type<int>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_integer() : Type
{
    return new IntegerType();
}

/**
 * @return Type<string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_string() : Type
{
    return new StringType();
}

/**
 * @return Type<float>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_float() : Type
{
    return new FloatType();
}

/**
 * @return Type<bool>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_boolean() : Type
{
    return new BooleanType();
}

/**
 * @template T of object
 *
 * @param class-string<T> $class
 *
 * @return Type<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_instance_of(string $class) : Type
{
    return new InstanceOfType($class);
}

/**
 * @return Type<object>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_object() : Type
{
    return new ObjectType();
}

/**
 * @return Type<bool|float|int|string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_scalar() : Type
{
    return new ScalarType();
}

/**
 * @return Type<resource>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_resource() : Type
{
    return new ResourceType();
}

/**
 * @return Type<array<mixed>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_array() : Type
{
    return new ArrayType();
}

/**
 * @return Type<callable>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_callable() : Type
{
    return new CallableType();
}

/**
 * @return Type<null>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_null() : Type
{
    return new NullType();
}

/**
 * @return Type<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_mixed() : Type
{
    return new MixedType();
}

/**
 * @return Type<int<0, max>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_positive_integer() : Type
{
    return new PositiveIntegerType();
}

/**
 * @return Type<non-empty-string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_non_empty_string() : Type
{
    return new NonEmptyStringType();
}

/**
 * @template T of UnitEnum
 *
 * @param class-string<T> $class
 *
 * @return Type<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_enum(string $class) : Type
{
    return new EnumType($class);
}

/**
 * @template T of bool|float|int|string
 *
 * @param T $value
 *
 * @return LiteralType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_literal(bool|float|int|string $value) : LiteralType
{
    return new LiteralType($value);
}

/**
 * @template T
 *
 * @param Type<T> $type
 * @param class-string<Type<mixed>> $typeClass
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_is(Type $type, string $typeClass) : bool
{
    return (new Comparator())->is($type, $typeClass);
}

/**
 * @template T
 *
 * @param Type<T> $type
 * @param class-string<Type<mixed>> $typeClass
 * @param class-string<Type<mixed>> ...$typeClasses
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_is_any(Type $type, string $typeClass, string ...$typeClasses) : bool
{
    return (new Comparator())->isAny($type, $typeClass, ...$typeClasses);
}

/**
 * @return Type<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function get_type(mixed $value) : Type
{
    return (new TypeDetector())->detectType($value);
}

/**
 * @template T of object
 *
 * @param null|class-string<T> $class
 *
 * @return ($class is null ? Type<class-string> : Type<class-string<T>>)
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_class_string(?string $class = null) : Type
{
    return new ClassStringType($class);
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function dom_element_to_string(\DOMElement $element, bool $format_output = false, bool $preserver_white_space = false) : string|false
{
    $doc = new \DOMDocument('1.0', 'UTF-8');
    $doc->formatOutput = $format_output;
    $doc->preserveWhiteSpace = $preserver_white_space;

    $importedNode = $doc->importNode($element, true);
    $doc->appendChild($importedNode);

    return $doc->saveXML($doc->documentElement);
}
