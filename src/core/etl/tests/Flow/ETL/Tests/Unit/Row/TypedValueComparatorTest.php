<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Dom\HTMLDocument;
use DOMDocument;
use Flow\ETL\Row\TypedValueComparator;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class TypedValueComparatorTest extends FlowTestCase
{
    public function test_date_equality_compares_the_day(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(
            type_date(),
            new DateTimeImmutable('2024-01-02 00:00:00', new DateTimeZone('UTC')),
            new DateTimeImmutable('2024-01-02 00:00:00', new DateTimeZone('UTC')),
        ));
        static::assertFalse($comparator->equals(
            type_date(),
            new DateTimeImmutable('2024-01-02 00:00:00', new DateTimeZone('UTC')),
            new DateTimeImmutable('2024-01-03 00:00:00', new DateTimeZone('UTC')),
        ));
    }

    public function test_float_equality_is_exact(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(type_float(), 1.5, 1.5));
        static::assertFalse($comparator->equals(type_float(), 1.5, 1.9));
        static::assertFalse($comparator->equals(type_float(), 0.1 + 0.2, 0.3));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_element_equality_compares_canonical_form(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(
            type_html_element(),
            type_html_element()->cast('<p>a</p>'),
            type_html_element()->cast('<p>a</p>'),
        ));
        static::assertFalse($comparator->equals(
            type_html_element(),
            type_html_element()->cast('<p>a</p>'),
            type_html_element()->cast('<p>b</p>'),
        ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_equality_compares_the_rendered_document(): void
    {
        $comparator = new TypedValueComparator();

        // @mago-ignore analysis:unavailable-method
        $left = HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body><p>a</p></body></html>');
        // @mago-ignore analysis:unavailable-method
        $right = HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body><p>a</p></body></html>');
        // @mago-ignore analysis:unavailable-method
        $other = HTMLDocument::createFromString('<!DOCTYPE html><html><head></head><body><p>b</p></body></html>');

        static::assertTrue($comparator->equals(type_html(), $left, $right));
        static::assertFalse($comparator->equals(type_html(), $left, $other));
    }

    public function test_json_equality_ignores_key_order(): void
    {
        static::assertTrue((new TypedValueComparator())->equals(
            type_json(),
            Json::fromString('{"a":1,"b":2}'),
            Json::fromString('{"b":2,"a":1}'),
        ));
    }

    public function test_list_equality_ignores_key_order(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(type_list(type_integer()), [1, 2], [1 => 2, 0 => 1]));
        static::assertFalse($comparator->equals(type_list(type_integer()), [1, 2], [1, 3]));
    }

    public function test_map_and_structure_equality_ignores_key_order(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(
            type_map(type_string(), type_integer()),
            ['a' => 1, 'b' => 2],
            ['b' => 2, 'a' => 1],
        ));
        static::assertTrue($comparator->equals(
            type_structure(['a' => type_integer(), 'b' => type_integer()]),
            ['a' => 1, 'b' => 2],
            ['b' => 2, 'a' => 1],
        ));
    }

    public function test_null_equals_null_only(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(type_string(), null, null));
        static::assertFalse($comparator->equals(type_string(), null, 'a'));
        static::assertFalse($comparator->equals(type_string(), 'a', null));
    }

    public function test_scalar_equality_is_identical(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(type_string(), 'a', 'a'));
        static::assertFalse($comparator->equals(type_string(), 'a', 'b'));
        static::assertTrue($comparator->equals(type_integer(), 1, 1));
        static::assertTrue($comparator->equals(type_boolean(), true, true));
        static::assertFalse($comparator->equals(type_boolean(), true, false));
    }

    public function test_time_equality_compares_the_duration(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(type_time(), new DateInterval('PT60M'), new DateInterval('PT1H')));
        static::assertFalse($comparator->equals(type_time(), new DateInterval('PT1H'), new DateInterval('PT2H')));
    }

    public function test_time_zone_equality_compares_the_name(): void
    {
        $comparator = new TypedValueComparator();

        static::assertTrue($comparator->equals(type_time_zone(), new DateTimeZone('UTC'), new DateTimeZone('UTC')));
        static::assertFalse($comparator->equals(
            type_time_zone(),
            new DateTimeZone('UTC'),
            new DateTimeZone('Europe/Warsaw'),
        ));
    }

    public function test_xml_equality_compares_canonical_form(): void
    {
        $comparator = new TypedValueComparator();

        $left = new DOMDocument();
        $left->loadXML('<root><a>1</a></root>');
        $right = new DOMDocument();
        $right->loadXML('<root><a>1</a></root>');
        $other = new DOMDocument();
        $other->loadXML('<root><a>2</a></root>');

        static::assertTrue($comparator->equals(type_xml(), $left, $right));
        static::assertFalse($comparator->equals(type_xml(), $left, $other));
    }

    public function test_xml_element_equality_compares_canonical_form(): void
    {
        $comparator = new TypedValueComparator();

        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a><b><a>1</a></b><a>2</a></root>');

        static::assertTrue($comparator->equals(
            type_xml_element(),
            type_xml_element()->assert($document->getElementsByTagName('a')->item(0)),
            type_xml_element()->assert($document->getElementsByTagName('a')->item(1)),
        ));
        static::assertFalse($comparator->equals(
            type_xml_element(),
            type_xml_element()->assert($document->getElementsByTagName('a')->item(0)),
            type_xml_element()->assert($document->getElementsByTagName('a')->item(2)),
        ));
    }

    public function test_two_date_time_immutable_of_the_same_instant_are_equal(): void
    {
        static::assertTrue((new TypedValueComparator())->equals(
            type_datetime(),
            new DateTimeImmutable('2024-01-02 03:04:05', new DateTimeZone('UTC')),
            new DateTimeImmutable('2024-01-02 03:04:05', new DateTimeZone('UTC')),
        ));
    }

    public function test_union_equality_resolves_the_member(): void
    {
        $comparator = new TypedValueComparator();
        $type = type_union(type_string(), type_integer());

        static::assertTrue($comparator->equals($type, 5, 5));
        static::assertFalse($comparator->equals($type, 5, '5'));
        static::assertTrue($comparator->equals($type, '5', '5'));
    }

    public function test_uuid_equality_compares_the_value(): void
    {
        static::assertTrue((new TypedValueComparator())->equals(
            type_uuid(),
            Uuid::fromString('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
            Uuid::fromString('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
        ));
    }
}
