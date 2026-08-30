<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Fixtures;

use DateTimeImmutable;
use Dom\HTMLDocument;
use Dom\HTMLElement;
use Dom\XMLDocument;
use Flow\Types\Tests\Unit\Type\Fixtures\Intersection\Date;
use Flow\Types\Tests\Unit\Type\Fixtures\Intersection\Time;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\structure_element;
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

/**
 * Pins the static shape `type_structure()` resolves to for every `type_*` DSL function.
 *
 * The analyzer derives a structure's shape from each element's type parameter. A `type_*` function whose return type
 * does not carry the value it represents in that parameter makes the derivation fail, and the shape silently degrades
 * to `array<array-key, mixed>` — valid PHP, passing tests, no analyzer error anywhere the shape is not consumed.
 *
 * The declared return types below only hold while that derivation is intact, so `just analyze` fails on a
 * regression. Narrowing another `type_*` return type without extending this fixture leaves that hole open.
 */
final class StructureShapeInference
{
    /**
     * @param array<string, mixed> $data
     *
     * @return array{list: list<string>, map: array<string, int>, nested: array{inner: int}, optional: string}
     */
    public function containers(array $data): array
    {
        return type_structure([
            'list' => type_list(type_string()),
            'map' => type_map(type_string(), type_integer()),
            'nested' => type_structure(['inner' => type_integer()]),
            'optional' => type_optional(type_string()),
        ])->assert($data);
    }

    /**
     * Pins the marker derivation the flow/types Mago plugin (flow-php/mago-types-bridge) provides:
     * the member value type comes from the element's first template and a literal `optional: true`
     * (TOptional) marks the key possibly undefined.
     *
     * @param array<string, mixed> $data
     *
     * @return array{id: int, interleaved?: string, name: string}
     */
    public function markerElements(array $data): array
    {
        return type_structure([
            'id' => type_integer(),
            'interleaved' => structure_element('interleaved', type_string(), optional: true),
            'name' => type_string(),
        ])->assert($data);
    }

    /**
     * Pins what the container types represent and expose, independently of `type_structure()`.
     *
     * @return array{list: list<string>, map: array<string, int>, element: string, key: string, value: int}
     */
    public function containerAccessors(mixed $list, mixed $map, mixed $element, mixed $key, mixed $value): array
    {
        return [
            'list' => type_list(type_string())->assert($list),
            'map' => type_map(type_string(), type_integer())->assert($map),
            'element' => type_list(type_string())->element()->assert($element),
            'key' => type_map(type_string(), type_integer())->key()->assert($key),
            'value' => type_map(type_string(), type_integer())->value()->assert($value),
        ];
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return array{enum: SomeEnum, instance_of: DateTimeImmutable, class_string: class-string, literal: 'fixed', union: int|string, intersection: mixed}
     */
    public function generics(array $data): array
    {
        return type_structure([
            'enum' => type_enum(SomeEnum::class),
            'instance_of' => type_instance_of(DateTimeImmutable::class),
            'class_string' => type_class_string(),
            'literal' => type_literal('fixed'),
            'union' => type_union(type_integer(), type_string()),
            'intersection' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
        ])->assert($data);
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return array{html: HTMLDocument, html_element: HTMLElement}
     */
    public function html(array $data): array
    {
        return type_structure([
            'html' => type_html(),
            'html_element' => type_html_element(),
        ])->assert($data);
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return array{json: Json, uuid: Uuid, datetime: \DateTimeInterface, date: \DateTimeInterface, time: \DateInterval, time_zone: \DateTimeZone, xml: \DOMDocument|XMLDocument, xml_element: \DOMElement|\Dom\Element}
     */
    public function objects(array $data): array
    {
        return type_structure([
            'json' => type_json(),
            'uuid' => type_uuid(),
            'datetime' => type_datetime(),
            'date' => type_date(),
            'time' => type_time(),
            'time_zone' => type_time_zone(),
            'xml' => type_xml(),
            'xml_element' => type_xml_element(),
        ])->assert($data);
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return array{integer: int, string: string, float: float, boolean: bool, numeric_string: numeric-string, non_empty_string: non-empty-string, positive_integer: int<0, max>, null: null, mixed: mixed, array: array<mixed>, object: object, scalar: bool|float|int|string, callable: callable, resource: resource}
     */
    public function scalars(array $data): array
    {
        return type_structure([
            'integer' => type_integer(),
            'string' => type_string(),
            'float' => type_float(),
            'boolean' => type_boolean(),
            'numeric_string' => type_numeric_string(),
            'non_empty_string' => type_non_empty_string(),
            'positive_integer' => type_positive_integer(),
            'null' => type_null(),
            'mixed' => type_mixed(),
            'array' => type_array(),
            'object' => type_object(),
            'scalar' => type_scalar(),
            'callable' => type_callable(),
            'resource' => type_resource(),
        ])->assert($data);
    }
}
