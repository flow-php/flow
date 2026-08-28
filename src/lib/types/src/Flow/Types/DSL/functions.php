<?php

declare(strict_types=1);

namespace Flow\Types\DSL;

use Dom\Element;
use Dom\HTMLDocument;
use Dom\HTMLElement;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\Types\Type;
use Flow\Types\Type\Comparator;
use Flow\Types\Type\Logical\ClassStringType;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\LiteralType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\NonEmptyStringType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\PositiveIntegerType;
use Flow\Types\Type\Logical\ScalarType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\CallableType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\IntersectionType;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\ObjectType;
use Flow\Types\Type\Native\ResourceType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\TypeDetector;
use Flow\Types\Type\TypeFactory;
use Flow\Types\Type\Types;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use UnitEnum;

/**
 * @template T
 *
 * @param array<array-key, Type<T>> $elements
 * @param array<array-key, Type<T>> $optional_elements
 *
 * @return StructureType<array<array-key, T>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_structure(array $elements = [], array $optional_elements = [], bool $allow_extra = false): StructureType
{
    return new StructureType($elements, $optional_elements, $allow_extra);
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
function type_union(Type $first, Type $second, Type ...$types): UnionType
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
 * @return IntersectionType<T, T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_intersection(Type $first, Type $second, Type ...$types): IntersectionType
{
    $type = new IntersectionType($first, $second);

    foreach ($types as $t) {
        $type = new IntersectionType($type, $t);
    }

    return $type;
}

/**
 * @return NumericStringType<numeric-string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_numeric_string(): NumericStringType
{
    return new NumericStringType();
}

/**
 * @template T
 *
 * @param Type<T> $type
 *
 * @return OptionalType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_optional(Type $type): OptionalType
{
    return new OptionalType($type);
}

/**
 * @param array<string, mixed> $data
 *
 * @return Type<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_from_array(array $data): Type
{
    return TypeFactory::fromArray($data);
}

/**
 * @template T
 *
 * @param Type<T> $type
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_is_nullable(Type $type): bool
{
    return (new Nullability())->is($type);
}

/**
 * Strip exactly one level of nullability, whichever of the two spellings carries it
 * (OptionalType, or a UnionType containing NullType). Total: a NOT NULL type is returned unchanged.
 *
 * @template T
 *
 * @param Type<T> $type
 *
 * @return Type<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_bare(Type $type): Type
{
    return (new Nullability())->bare($type);
}

/**
 * @param Type<mixed> $left
 * @param Type<mixed> $right
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_equals(Type $left, Type $right): bool
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
function types(Type ...$types): Types
{
    return new Types(...$types);
}

/**
 * @template T
 *
 * @param Type<T> $element
 *
 * @return ListType<list<T>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_list(Type $element): ListType
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
 * @return MapType<array<TKey, TValue>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_map(Type $key_type, Type $value_type): MapType
{
    return new MapType($key_type, $value_type);
}

/**
 * @return JsonType<Json>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_json(): JsonType
{
    return new JsonType();
}

/**
 * @return DateTimeType<\DateTimeInterface>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_datetime(): DateTimeType
{
    return new DateTimeType();
}

/**
 * @return DateType<\DateTimeInterface>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_date(): DateType
{
    return new DateType();
}

/**
 * @return TimeType<\DateInterval>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_time(): TimeType
{
    return new TimeType();
}

/**
 * @return TimeZoneType<\DateTimeZone>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_time_zone(): TimeZoneType
{
    return new TimeZoneType();
}

/**
 * @return XMLType<\DOMDocument|XMLDocument>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_xml(): XMLType
{
    return new XMLType();
}

/**
 * @return XMLElementType<\DOMElement|Element>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_xml_element(): XMLElementType
{
    return new XMLElementType();
}

/**
 * @return UuidType<Uuid>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_uuid(): UuidType
{
    return new UuidType();
}

/**
 * @return IntegerType<int>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_integer(): IntegerType
{
    return new IntegerType();
}

/**
 * @return StringType<string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_string(): StringType
{
    return new StringType();
}

/**
 * @return FloatType<float>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_float(): FloatType
{
    return new FloatType();
}

/**
 * @return BooleanType<bool>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_boolean(): BooleanType
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
function type_instance_of(string $class): InstanceOfType
{
    return new InstanceOfType($class);
}

/**
 * @return ObjectType<object>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_object(): ObjectType
{
    return new ObjectType();
}

/**
 * @return ScalarType<bool|float|int|string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_scalar(): ScalarType
{
    return new ScalarType();
}

/**
 * @return ResourceType<resource>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_resource(): ResourceType
{
    return new ResourceType();
}

/**
 * @return ArrayType<array<mixed>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_array(): ArrayType
{
    return new ArrayType();
}

/**
 * @return CallableType<callable>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_callable(): CallableType
{
    return new CallableType();
}

/**
 * @return NullType<null>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_null(): NullType
{
    return new NullType();
}

/**
 * @return MixedType<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_mixed(): MixedType
{
    return new MixedType();
}

/**
 * @return PositiveIntegerType<int<0, max>>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_positive_integer(): PositiveIntegerType
{
    return new PositiveIntegerType();
}

/**
 * @return NonEmptyStringType<non-empty-string>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_non_empty_string(): NonEmptyStringType
{
    return new NonEmptyStringType();
}

/**
 * @return EmptyArrayType<array{}>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_empty_array(): EmptyArrayType
{
    return new EmptyArrayType();
}

/**
 * @template T of UnitEnum
 *
 * @param class-string<T> $class
 *
 * @return EnumType<T>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_enum(string $class): EnumType
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
function type_literal(bool|float|int|string $value): LiteralType
{
    return new LiteralType($value);
}

/**
 * @return HTMLType<HTMLDocument>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_html(): HTMLType
{
    return new HTMLType();
}

/**
 * @return HTMLElementType<HTMLElement>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_html_element(): HTMLElementType
{
    return new HTMLElementType();
}

/**
 * @template T
 *
 * @param Type<T> $type
 * @param class-string<Type<mixed>> $typeClass
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function type_is(Type $type, string $typeClass): bool
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
function type_is_any(Type $type, string $typeClass, string ...$typeClasses): bool
{
    return (new Comparator())->isAny($type, $typeClass, ...$typeClasses);
}

/**
 * @return Type<mixed>
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function get_type(mixed $value): Type
{
    return (new TypeDetector())->detectType($value);
}

/**
 * @template T of object
 *
 * @param null|class-string<T> $class
 *
 * @return ($class is null ? ClassStringType<class-string> : ClassStringType<class-string<T>>)
 */
#[DocumentationDSL(module: Module::TYPES, type: DSLType::TYPE)]
function type_class_string(?string $class = null): ClassStringType
{
    return new ClassStringType($class);
}

#[DocumentationDSL(module: Module::TYPES, type: DSLType::HELPER)]
function dom_element_to_string(
    DOMElement $element,
    bool $format_output = false,
    bool $preserver_white_space = false,
): string|false {
    $doc = new DOMDocument('1.0', 'UTF-8');
    $doc->formatOutput = $format_output;
    $doc->preserveWhiteSpace = $preserver_white_space;

    $importedNode = $doc->importNode($element, true);

    if ($importedNode === false) {
        return false;
    }

    $doc->appendChild($importedNode);

    return $doc->saveXML($doc->documentElement);
}
