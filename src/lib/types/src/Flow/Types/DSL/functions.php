<?php

declare(strict_types=1);

namespace Flow\Types\DSL;

use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\Types\Type\{Comparator, Type, TypeDetector, TypeFactory, Types};
use Flow\Types\Type\Logical\{DateTimeType,
    DateType,
    InstanceOfType,
    JsonType,
    ListType,
    MapType,
    NonEmptyStringType,
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
    NullType,
    ObjectType,
    ResourceType,
    StringType,
    UnionType};
use UnitEnum;

/**
 * @template T of array
 *
 * @param T $elements
 *
 * @return StructureType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_structure(array $elements) : StructureType
{
    return new StructureType($elements);
}

/**
 * @template T
 *
 * @param Type<T> $first
 * @param Type<T> $second
 * @param Type<T> ...$types
 *
 * @return UnionType<T, T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_union(Type $first, Type $second, Type ...$types) : UnionType
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
 * @param Type<T> $type
 *
 * @return OptionalType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_optional(Type $type) : OptionalType
{
    return new OptionalType($type);
}

/**
 * @param array<mixed> $data
 *
 * @return Type<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_from_array(array $data) : Type
{
    return TypeFactory::fromArray($data);
}

/**
 * @param Type<mixed> $type
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
 * @template TLeft
 * @template TRight
 *
 * @param Type<TLeft> $left
 * @param Type<TRight> $right
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_equals(Type $left, Type $right) : bool
{
    return (new Comparator())->equals($left, $right);
}

/**
 * @param Type<mixed> ...$types
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
 * @template T
 *
 * @param Type<T> $value_type
 *
 * @return MapType<array-key, T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_map(StringType|IntegerType $key_type, Type $value_type) : MapType
{
    return new MapType($key_type, $value_type);
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_json() : JsonType
{
    return new JsonType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_datetime() : DateTimeType
{
    return new DateTimeType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_date() : DateType
{
    return new DateType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_time() : TimeType
{
    return new TimeType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_xml() : XMLType
{
    return new XMLType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_xml_element() : XMLElementType
{
    return new XMLElementType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_uuid() : UuidType
{
    return new UuidType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_integer() : IntegerType
{
    return new IntegerType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_string() : StringType
{
    return new StringType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_float() : FloatType
{
    return new FloatType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_boolean() : BooleanType
{
    return new BooleanType();
}

/**
 * @template T of object
 *
 * @param class-string<T> $class
 *
 * @return InstanceOfType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_instance_of(string $class) : InstanceOfType
{
    return new InstanceOfType($class);
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_object() : ObjectType
{
    return new ObjectType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_scalar() : ScalarType
{
    return new ScalarType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_resource() : ResourceType
{
    return new ResourceType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_array() : ArrayType
{
    return new ArrayType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_callable() : CallableType
{
    return new CallableType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_null() : NullType
{
    return new NullType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_positive_integer() : PositiveIntegerType
{
    return new PositiveIntegerType();
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_non_empty_string() : NonEmptyStringType
{
    return new NonEmptyStringType();
}

/**
 * @template T of UnitEnum
 *
 * @param class-string<T> $class
 *
 * @return EnumType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_enum(string $class) : EnumType
{
    return new EnumType($class);
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
