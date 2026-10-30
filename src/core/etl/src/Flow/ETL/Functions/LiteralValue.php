<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeDetector;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Throwable;
use UnitEnum;

use function array_key_exists;
use function defined;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function in_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_infinite;
use function is_int;
use function is_nan;
use function is_string;

use const INF;
use const NAN;

/**
 * The closed {type, value} codec of a Literal's value: the type is what TypeDetector detects, the value is a JSON
 * shape it decodes back from - never Type::cast(), which loses non-backed enums, float formatting and zone names.
 */
final readonly class LiteralValue
{
    /**
     * @param array<array-key, mixed> $encoded the shape encode() returns, after a JSON round trip
     *
     * @throws InvalidArgumentException on a shape encode() never writes
     */
    public function decode(array $encoded): mixed
    {
        if (!array_key_exists('type', $encoded) || !array_key_exists('value', $encoded)) {
            throw new InvalidArgumentException('A literal value is encoded as {type, value}');
        }

        $type = type_from_array(type_map(type_string(), type_mixed())->assert($encoded['type']));
        /** @var mixed $value */
        $value = $encoded['value'];

        if ($type instanceof NullType) {
            if ($value !== null) {
                throw new InvalidArgumentException('A null literal carries no value');
            }

            return null;
        }

        if ($type instanceof BooleanType) {
            return type_boolean()->assert($value);
        }

        if ($type instanceof IntegerType) {
            return type_integer()->assert($value);
        }

        if ($type instanceof StringType) {
            return type_string()->assert($value);
        }

        if ($type instanceof FloatType) {
            return (
                is_string($value)
                    ? match ($value) {
                        'NAN' => NAN,
                        'INF' => INF,
                        '-INF' => -INF,
                        default => throw InvalidArgumentException::because('"%s" is not a float literal', $value),
                    }
                    : type_float()->assert($value)
            );
        }

        if ($type instanceof DateType || $type instanceof DateTimeType) {
            $dateTime = type_structure([
                'class' => type_string(),
                'datetime' => type_string(),
                'zone' => type_string(),
            ])->assert($value);

            if (!in_array($dateTime['class'], [DateTimeImmutable::class, DateTime::class], true)) {
                throw InvalidArgumentException::because('%s is not a date time literal', $dateTime['class']);
            }

            return $dateTime['class'] === DateTime::class
                ? new DateTime($dateTime['datetime'], new DateTimeZone($dateTime['zone']))
                : new DateTimeImmutable($dateTime['datetime'], new DateTimeZone($dateTime['zone']));
        }

        if ($type instanceof TimeZoneType) {
            return new DateTimeZone(type_string()->assert($value));
        }

        if ($type instanceof EnumType) {
            $case = $type->class . '::' . type_string()->assert($value);

            if (!defined($case)) {
                throw InvalidArgumentException::because('%s is not a case of %s', $case, $type->class);
            }

            return $type->assert(constant($case));
        }

        if ($type instanceof InstanceOfType) {
            return type_from_array(type_map(type_string(), type_mixed())->assert($value));
        }

        if ($type instanceof JsonType) {
            return new Json(type_string()->assert($value));
        }

        if ($type instanceof UuidType) {
            return new Uuid(type_string()->assert($value));
        }

        if (
            $type instanceof ListType
            || $type instanceof MapType
            || $type instanceof StructureType
            || $type instanceof ArrayType
        ) {
            $elements = [];

            // @mago-ignore analysis:mixed-assignment
            foreach (type_array()->assert($value) as $key => $element) {
                $elements[$key] = $this->decode(type_array()->assert($element));
            }

            return $elements;
        }

        throw InvalidArgumentException::because('A literal of type %s cannot be decoded', $type->toString());
    }

    /**
     * @return null|array{type: array<array-key, mixed>, value: mixed} null when $value has no encoding (a DOM node,
     *                                                                 any other object, a value TypeDetector refuses)
     */
    public function encode(mixed $value): ?array
    {
        try {
            $type = (new TypeDetector())->detectType($value);
        } catch (Throwable) {
            return null;
        }

        if ($value === null || is_bool($value) || is_int($value) || is_string($value)) {
            return ['type' => $type->normalize(), 'value' => $value];
        }

        if (is_float($value)) {
            return [
                'type' => $type->normalize(),
                'value' => match (true) {
                    is_nan($value) => 'NAN',
                    is_infinite($value) => $value > 0 ? 'INF' : '-INF',
                    default => $value,
                },
            ];
        }

        if ($value instanceof DateTimeInterface && ($type instanceof DateType || $type instanceof DateTimeType)) {
            return [
                'type' => $type->normalize(),
                'value' => [
                    'class' => $value::class,
                    'datetime' => $value->format('Y-m-d\TH:i:s.u'),
                    'zone' => $value->getTimezone()->getName(),
                ],
            ];
        }

        if ($value instanceof DateTimeZone && $type instanceof TimeZoneType) {
            return ['type' => $type->normalize(), 'value' => $value->getName()];
        }

        if ($value instanceof UnitEnum && $type instanceof EnumType) {
            return ['type' => $type->normalize(), 'value' => $value->name];
        }

        if ($value instanceof Type && $type instanceof InstanceOfType) {
            return ['type' => $type->normalize(), 'value' => $value->normalize()];
        }

        if (
            $value instanceof Json && $type instanceof JsonType
            || $value instanceof Uuid && $type instanceof UuidType
        ) {
            return ['type' => $type->normalize(), 'value' => $value->toString()];
        }

        if (is_array($value)) {
            $elements = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $key => $element) {
                $encoded = $this->encode($element);

                if ($encoded === null) {
                    return null;
                }

                $elements[$key] = $encoded;
            }

            return ['type' => $type->normalize(), 'value' => $elements];
        }

        return null;
    }
}
