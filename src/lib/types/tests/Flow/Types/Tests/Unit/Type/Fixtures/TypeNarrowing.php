<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Fixtures;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Dom\Element;
use Dom\HTMLDocument;
use Dom\HTMLElement;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Flow\Types\Tests\Unit\Type\Fixtures\Intersection\Date;
use Flow\Types\Tests\Unit\Type\Fixtures\Intersection\Time;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_class_string;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_intersection;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_literal;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_resource;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

function sink_array(array $v): array
{
    return $v;
}

function sink_bool(bool $v): bool
{
    return $v;
}

function sink_callable(callable $v): callable
{
    return $v;
}

/**
 * @param class-string $v
 *
 * @return class-string
 */
function sink_class_string(string $v): string
{
    return $v;
}

function sink_datetime(DateTimeInterface $v): DateTimeInterface
{
    return $v;
}

function sink_date_immutable(DateTimeImmutable $v): DateTimeImmutable
{
    return $v;
}

function sink_enum(SomeEnum $v): SomeEnum
{
    return $v;
}

function sink_float(float $v): float
{
    return $v;
}

function sink_html(HTMLDocument $v): HTMLDocument
{
    return $v;
}

function sink_html_element(HTMLElement $v): HTMLElement
{
    return $v;
}

function sink_int(int $v): int
{
    return $v;
}

/**
 * @param Date&Time $v
 *
 * @return Date&Time
 */
function sink_intersection(object $v): object
{
    return $v;
}

function sink_json(Json $v): Json
{
    return $v;
}

/**
 * @param list<int> $v
 *
 * @return list<int>
 */
function sink_list_of_int(array $v): array
{
    return $v;
}

/**
 * @param 'fixed' $v
 *
 * @return 'fixed'
 */
function sink_literal(string $v): string
{
    return $v;
}

/**
 * @param array<string, int> $v
 *
 * @return array<string, int>
 */
function sink_map(array $v): array
{
    return $v;
}

function sink_mixed(mixed $v): mixed
{
    return $v;
}

function sink_null(null $v): null
{
    return $v;
}

/**
 * @param non-empty-string $v
 *
 * @return non-empty-string
 */
function sink_non_empty_string(string $v): string
{
    return $v;
}

/**
 * @param numeric-string $v
 *
 * @return numeric-string
 */
function sink_numeric_string(string $v): string
{
    return $v;
}

function sink_object(object $v): object
{
    return $v;
}

function sink_optional_int(?int $v): ?int
{
    return $v;
}

/**
 * @param int<0, max> $v
 *
 * @return int<0, max>
 */
function sink_positive_int(int $v): int
{
    return $v;
}

/**
 * @param resource $v
 *
 * @return resource
 */
function sink_resource($v): mixed
{
    return $v;
}

function sink_scalar(bool|float|int|string $v): bool|float|int|string
{
    return $v;
}

/**
 * @param array{id: int} $v
 *
 * @return array{id: int}
 */
function sink_structure(array $v): array
{
    return $v;
}

function sink_string(string $v): string
{
    return $v;
}

function sink_time(DateInterval $v): DateInterval
{
    return $v;
}

function sink_time_zone(DateTimeZone $v): DateTimeZone
{
    return $v;
}

function sink_union(int|string $v): int|string
{
    return $v;
}

function sink_uuid(Uuid $v): Uuid
{
    return $v;
}

/**
 * @param DOMDocument|XMLDocument $v
 *
 * @return DOMDocument|XMLDocument
 */
function sink_xml(object $v): object
{
    return $v;
}

/**
 * @param DOMElement|Element $v
 *
 * @return DOMElement|Element
 */
function sink_xml_element(object $v): object
{
    return $v;
}

/**
 * Pins the static type every `Type` implementation returns from `assert()` and `cast()`, and narrows to in `isValid()`.
 *
 * Erasure produces no diagnostic on its own — an `assert()` declaring `non-empty-string` but resolving to `string` is
 * a valid program. Each call below feeds a sink accepting only the narrow type, so `just analyze` fails on the line
 * where a narrowing is lost. Mago reads a return from the implementation's own `@return`, never the interface's.
 *
 * One fresh parameter per call: a reused variable carries the first call's narrowing into the second and hides it.
 */
final class TypeNarrowing
{
    /**
     * @return array<string, mixed>
     */
    public function assertNarrowsLogical(
        mixed $classString,
        mixed $dateTime,
        mixed $date,
        mixed $instanceOf,
        mixed $json,
        mixed $list,
        mixed $literal,
        mixed $map,
        mixed $nonEmptyString,
        mixed $numericString,
        mixed $optional,
        mixed $positiveInteger,
        mixed $scalar,
        mixed $structure,
        mixed $time,
        mixed $timeZone,
        mixed $uuid,
        mixed $xmlElement,
        mixed $xml,
    ): array {
        return [
            'class_string' => sink_class_string(type_class_string()->assert($classString)),
            'date_time' => sink_datetime(type_datetime()->assert($dateTime)),
            'date' => sink_datetime(type_date()->assert($date)),
            'instance_of' => sink_date_immutable(type_instance_of(DateTimeImmutable::class)->assert($instanceOf)),
            'json' => sink_json(type_json()->assert($json)),
            'list' => sink_list_of_int(type_list(type_integer())->assert($list)),
            'literal' => sink_literal(type_literal('fixed')->assert($literal)),
            'map' => sink_map(type_map(type_string(), type_integer())->assert($map)),
            'non_empty_string' => sink_non_empty_string(type_non_empty_string()->assert($nonEmptyString)),
            'numeric_string' => sink_numeric_string(type_numeric_string()->assert($numericString)),
            'optional' => sink_optional_int(type_optional(type_integer())->assert($optional)),
            'positive_integer' => sink_positive_int(type_positive_integer()->assert($positiveInteger)),
            'scalar' => sink_scalar(type_scalar()->assert($scalar)),
            'structure' => sink_structure(type_structure(['id' => type_integer()])->assert($structure)),
            'time' => sink_time(type_time()->assert($time)),
            'time_zone' => sink_time_zone(type_time_zone()->assert($timeZone)),
            'uuid' => sink_uuid(type_uuid()->assert($uuid)),
            'xml_element' => sink_xml_element(type_xml_element()->assert($xmlElement)),
            'xml' => sink_xml(type_xml()->assert($xml)),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function assertNarrowsNative(
        mixed $array,
        mixed $boolean,
        mixed $callable,
        mixed $enum,
        mixed $float,
        mixed $integer,
        mixed $intersection,
        mixed $mixed,
        mixed $null,
        mixed $object,
        mixed $resource,
        mixed $string,
        mixed $union,
    ): array {
        return [
            'array' => sink_array(type_array()->assert($array)),
            'boolean' => sink_bool(type_boolean()->assert($boolean)),
            'callable' => sink_callable(type_callable()->assert($callable)),
            'enum' => sink_enum(type_enum(SomeEnum::class)->assert($enum)),
            'float' => sink_float(type_float()->assert($float)),
            'integer' => sink_int(type_integer()->assert($integer)),
            // IntersectionType's TRight never binds, so this resolves to `(Date&TRight)|(Time&TRight)`.
            // @mago-expect analysis:less-specific-nested-argument-type
            'intersection' => sink_intersection(type_intersection(
                type_instance_of(Date::class),
                type_instance_of(Time::class),
            )->assert($intersection)),
            'mixed' => sink_mixed(type_mixed()->assert($mixed)),
            'null' => sink_null(type_null()->assert($null)),
            'object' => sink_object(type_object()->assert($object)),
            'resource' => sink_resource(type_resource()->assert($resource)),
            'string' => sink_string(type_string()->assert($string)),
            'union' => sink_union(type_union(type_integer(), type_string())->assert($union)),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function castNarrowsLogical(
        mixed $classString,
        mixed $dateTime,
        mixed $date,
        mixed $instanceOf,
        mixed $json,
        mixed $list,
        mixed $literal,
        mixed $map,
        mixed $nonEmptyString,
        mixed $numericString,
        mixed $optional,
        mixed $positiveInteger,
        mixed $scalar,
        mixed $structure,
        mixed $time,
        mixed $timeZone,
        mixed $uuid,
        mixed $xmlElement,
        mixed $xml,
    ): array {
        return [
            'class_string' => sink_class_string(type_class_string()->cast($classString)),
            'date_time' => sink_datetime(type_datetime()->cast($dateTime)),
            'date' => sink_datetime(type_date()->cast($date)),
            'instance_of' => sink_date_immutable(type_instance_of(DateTimeImmutable::class)->cast($instanceOf)),
            'json' => sink_json(type_json()->cast($json)),
            'list' => sink_list_of_int(type_list(type_integer())->cast($list)),
            'literal' => sink_literal(type_literal('fixed')->cast($literal)),
            'map' => sink_map(type_map(type_string(), type_integer())->cast($map)),
            'non_empty_string' => sink_non_empty_string(type_non_empty_string()->cast($nonEmptyString)),
            'numeric_string' => sink_numeric_string(type_numeric_string()->cast($numericString)),
            'optional' => sink_optional_int(type_optional(type_integer())->cast($optional)),
            'positive_integer' => sink_positive_int(type_positive_integer()->cast($positiveInteger)),
            'scalar' => sink_scalar(type_scalar()->cast($scalar)),
            'structure' => sink_structure(type_structure(['id' => type_integer()])->cast($structure)),
            'time' => sink_time(type_time()->cast($time)),
            'time_zone' => sink_time_zone(type_time_zone()->cast($timeZone)),
            'uuid' => sink_uuid(type_uuid()->cast($uuid)),
            'xml_element' => sink_xml_element(type_xml_element()->cast($xmlElement)),
            'xml' => sink_xml(type_xml()->cast($xml)),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function castNarrowsNative(
        mixed $array,
        mixed $boolean,
        mixed $callable,
        mixed $enum,
        mixed $float,
        mixed $integer,
        mixed $intersection,
        mixed $mixed,
        mixed $null,
        mixed $object,
        mixed $resource,
        mixed $string,
        mixed $union,
    ): array {
        return [
            'array' => sink_array(type_array()->cast($array)),
            'boolean' => sink_bool(type_boolean()->cast($boolean)),
            'callable' => sink_callable(type_callable()->cast($callable)),
            'enum' => sink_enum(type_enum(SomeEnum::class)->cast($enum)),
            'float' => sink_float(type_float()->cast($float)),
            'integer' => sink_int(type_integer()->cast($integer)),
            // IntersectionType's TRight never binds, so this resolves to `(Date&TRight)|(Time&TRight)`.
            // @mago-expect analysis:less-specific-nested-argument-type
            'intersection' => sink_intersection(type_intersection(
                type_instance_of(Date::class),
                type_instance_of(Time::class),
            )->cast($intersection)),
            'mixed' => sink_mixed(type_mixed()->cast($mixed)),
            'null' => sink_null(type_null()->cast($null)),
            'object' => sink_object(type_object()->cast($object)),
            'resource' => sink_resource(type_resource()->cast($resource)),
            'string' => sink_string(type_string()->cast($string)),
            'union' => sink_union(type_union(type_integer(), type_string())->cast($union)),
        ];
    }

    /**
     * `type_mixed()` is absent deliberately: narrowing to `mixed` asserts nothing, so a sink cannot observe it.
     *
     * @return array<string, mixed>
     */
    public function isValidNarrowsLogical(
        mixed $classString,
        mixed $dateTime,
        mixed $date,
        mixed $instanceOf,
        mixed $json,
        mixed $list,
        mixed $literal,
        mixed $map,
        mixed $nonEmptyString,
        mixed $numericString,
        mixed $optional,
        mixed $positiveInteger,
        mixed $scalar,
        mixed $structure,
        mixed $time,
        mixed $timeZone,
        mixed $uuid,
        mixed $xmlElement,
        mixed $xml,
    ): array {
        $narrowed = [];

        if (type_class_string()->isValid($classString)) {
            $narrowed['class_string'] = sink_class_string($classString);
        }

        if (type_datetime()->isValid($dateTime)) {
            $narrowed['date_time'] = sink_datetime($dateTime);
        }

        if (type_date()->isValid($date)) {
            $narrowed['date'] = sink_datetime($date);
        }

        if (type_instance_of(DateTimeImmutable::class)->isValid($instanceOf)) {
            $narrowed['instance_of'] = sink_date_immutable($instanceOf);
        }

        if (type_json()->isValid($json)) {
            $narrowed['json'] = sink_json($json);
        }

        if (type_list(type_integer())->isValid($list)) {
            $narrowed['list'] = sink_list_of_int($list);
        }

        if (type_literal('fixed')->isValid($literal)) {
            $narrowed['literal'] = sink_literal($literal);
        }

        if (type_map(type_string(), type_integer())->isValid($map)) {
            $narrowed['map'] = sink_map($map);
        }

        if (type_non_empty_string()->isValid($nonEmptyString)) {
            $narrowed['non_empty_string'] = sink_non_empty_string($nonEmptyString);
        }

        if (type_numeric_string()->isValid($numericString)) {
            $narrowed['numeric_string'] = sink_numeric_string($numericString);
        }

        if (type_optional(type_integer())->isValid($optional)) {
            $narrowed['optional'] = sink_optional_int($optional);
        }

        if (type_positive_integer()->isValid($positiveInteger)) {
            $narrowed['positive_integer'] = sink_positive_int($positiveInteger);
        }

        if (type_scalar()->isValid($scalar)) {
            $narrowed['scalar'] = sink_scalar($scalar);
        }

        if (type_structure(['id' => type_integer()])->isValid($structure)) {
            $narrowed['structure'] = sink_structure($structure);
        }

        if (type_time()->isValid($time)) {
            $narrowed['time'] = sink_time($time);
        }

        if (type_time_zone()->isValid($timeZone)) {
            $narrowed['time_zone'] = sink_time_zone($timeZone);
        }

        if (type_uuid()->isValid($uuid)) {
            $narrowed['uuid'] = sink_uuid($uuid);
        }

        if (type_xml_element()->isValid($xmlElement)) {
            $narrowed['xml_element'] = sink_xml_element($xmlElement);
        }

        if (type_xml()->isValid($xml)) {
            $narrowed['xml'] = sink_xml($xml);
        }

        return $narrowed;
    }

    /**
     * @return array<string, mixed>
     */
    public function isValidNarrowsNative(
        mixed $array,
        mixed $boolean,
        mixed $callable,
        mixed $enum,
        mixed $float,
        mixed $integer,
        mixed $intersection,
        mixed $null,
        mixed $object,
        mixed $resource,
        mixed $string,
        mixed $union,
    ): array {
        $narrowed = [];

        if (type_array()->isValid($array)) {
            $narrowed['array'] = sink_array($array);
        }

        if (type_boolean()->isValid($boolean)) {
            $narrowed['boolean'] = sink_bool($boolean);
        }

        if (type_callable()->isValid($callable)) {
            $narrowed['callable'] = sink_callable($callable);
        }

        if (type_enum(SomeEnum::class)->isValid($enum)) {
            $narrowed['enum'] = sink_enum($enum);
        }

        if (type_float()->isValid($float)) {
            $narrowed['float'] = sink_float($float);
        }

        if (type_integer()->isValid($integer)) {
            $narrowed['integer'] = sink_int($integer);
        }

        if (type_intersection(type_instance_of(Date::class), type_instance_of(Time::class))->isValid($intersection)) {
            // IntersectionType's TRight never binds, so this resolves to `(Date&TRight)|(Time&TRight)`.
            // @mago-expect analysis:less-specific-nested-argument-type
            $narrowed['intersection'] = sink_intersection($intersection);
        }

        if (type_null()->isValid($null)) {
            $narrowed['null'] = sink_null($null);
        }

        if (type_object()->isValid($object)) {
            $narrowed['object'] = sink_object($object);
        }

        if (type_resource()->isValid($resource)) {
            $narrowed['resource'] = sink_resource($resource);
        }

        if (type_string()->isValid($string)) {
            $narrowed['string'] = sink_string($string);
        }

        if (type_union(type_integer(), type_string())->isValid($union)) {
            $narrowed['union'] = sink_union($union);
        }

        return $narrowed;
    }

    /**
     * Split out of the methods above because `Dom\HTMLDocument` and `Dom\HTMLElement` only exist on PHP >= 8.4;
     * keeping them here lets the rest stay executable on 8.3.
     *
     * @return array<string, mixed>
     */
    public function narrowsHtml(
        mixed $assertedHtml,
        mixed $assertedHtmlElement,
        mixed $castHtml,
        mixed $castHtmlElement,
        mixed $validHtml,
        mixed $validHtmlElement,
    ): array {
        $narrowed = [
            'asserted_html' => sink_html(type_html()->assert($assertedHtml)),
            'asserted_html_element' => sink_html_element(type_html_element()->assert($assertedHtmlElement)),
            'cast_html' => sink_html(type_html()->cast($castHtml)),
            'cast_html_element' => sink_html_element(type_html_element()->cast($castHtmlElement)),
        ];

        if (type_html()->isValid($validHtml)) {
            $narrowed['valid_html'] = sink_html($validHtml);
        }

        if (type_html_element()->isValid($validHtmlElement)) {
            $narrowed['valid_html_element'] = sink_html_element($validHtmlElement);
        }

        return $narrowed;
    }
}
