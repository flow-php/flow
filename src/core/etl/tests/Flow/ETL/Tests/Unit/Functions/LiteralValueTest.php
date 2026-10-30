<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Foundation\Describe\Json;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ArraySort\Sort;
use Flow\ETL\Functions\Between\Boundary;
use Flow\ETL\Functions\LiteralValue;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json as JsonValue;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function is_float;
use function is_nan;
use function var_export;

use const INF;
use const NAN;

final class LiteralValueTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{mixed}>
     */
    public static function values(): Generator
    {
        yield 'null' => [null];
        yield 'bool' => [true];
        yield 'int' => [42];
        yield 'string' => ['abc'];
        yield 'float' => [1.5];
        yield 'float with a zero fraction' => [1.0];
        yield 'NAN' => [NAN];
        yield 'INF' => [INF];
        yield '-INF' => [-INF];
        yield 'datetime in Warsaw before DST' => [new DateTimeImmutable(
            '2026-03-29 01:30:00.123456',
            new DateTimeZone('Europe/Warsaw'),
        )];
        yield 'datetime in Warsaw after DST' => [new DateTimeImmutable(
            '2026-03-29 03:30:00.654321',
            new DateTimeZone('Europe/Warsaw'),
        )];
        yield 'mutable datetime' => [new DateTime('2026-01-02 03:04:05', new DateTimeZone('UTC'))];
        yield 'midnight datetime (a date)' => [new DateTimeImmutable('2026-01-02 00:00:00', new DateTimeZone('UTC'))];
        yield 'datetime with an offset zone' => [new DateTimeImmutable('2026-01-02 03:04:05+02:00')];
        yield 'time zone' => [new DateTimeZone('Europe/Warsaw')];
        yield 'backed enum' => [Sort::asort];
        yield 'non-backed enum' => [Boundary::INCLUSIVE];
        yield 'type' => [type_list(type_integer())];
        yield 'json' => [new JsonValue('{"a":1}')];
        yield 'uuid' => [new Uuid('0196fb5a-7c5f-7a2e-9e0c-5b0b1a6c9f21')];
        yield 'empty array' => [[]];
        yield 'mixed array' => [[1, 'a']];
        yield 'list of numbers' => [[1, 2.5]];
        yield 'structure' => [['a' => 1, 'b' => 'x']];
        yield 'nested map with a datetime' => [['when' => ['at' => new DateTimeImmutable(
            '2026-01-02 03:04:05',
            new DateTimeZone('UTC'),
        )]]];
    }

    #[DataProvider('values')]
    public function test_a_value_round_trips(mixed $value): void
    {
        $codec = new LiteralValue();
        $encoded = $codec->encode($value);

        static::assertNotNull($encoded);

        // @mago-ignore analysis:mixed-assignment
        $decoded = $codec->decode(Json::decode(Json::encode($encoded)));

        if (is_float($value) && is_nan($value)) {
            static::assertTrue(is_float($decoded) && is_nan($decoded));

            return;
        }

        static::assertSame(var_export($value, true), var_export($decoded, true));
    }

    public function test_a_dom_node_does_not_encode(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a></root>');

        static::assertNull((new LiteralValue())->encode($document->documentElement));
    }

    public function test_an_array_with_a_dom_node_does_not_encode(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a></root>');

        static::assertNull((new LiteralValue())->encode(['node' => $document->documentElement]));
    }

    public function test_decode_refuses_an_unknown_shape(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new LiteralValue())->decode(['type' => ['type' => 'float'], 'value' => 'not a float']);
    }

    public function test_decode_refuses_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A literal value is encoded as {type, value}');

        (new LiteralValue())->decode(['type' => ['type' => 'integer']]);
    }

    public function test_decode_refuses_an_unknown_enum_case(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new LiteralValue())->decode(['type' => ['type' => 'enum', 'class' => Boundary::class], 'value' => 'NOPE']);
    }
}
