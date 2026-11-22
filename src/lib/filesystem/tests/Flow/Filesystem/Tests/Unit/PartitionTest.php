<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use DateTimeImmutable;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Partition;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

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
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;
use function ini_get;
use function ini_set;

final class PartitionTest extends TestCase
{
    public static function unpartitionable_types(): Generator
    {
        yield 'html' => [type_html(), 'html'];
        yield 'html element' => [type_html_element(), 'html_element'];
        yield 'xml' => [type_xml(), 'xml'];
        yield 'xml element' => [type_xml_element(), 'xml_element'];
        yield 'json' => [type_json(), 'json'];
        yield 'list' => [type_list(type_integer()), 'list<integer>'];
        yield 'structure' => [type_structure(['a' => type_integer()]), 'structure{a: integer}'];
        yield 'map' => [type_map(type_string(), type_integer()), 'map<string, integer>'];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('unpartitionable_types')]
    public function test_assert_partitionable_refuses_a_type_without_a_folder_name(Type $type, string $name): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "c" of type ' . $name . ' can\'t be used as a partition');

        Partition::assertPartitionable('c', $type);
    }

    public function test_assert_partitionable_accepts_a_scalar_type(): void
    {
        Partition::assertPartitionable('c', type_float());

        $this->addToAssertionCount(1);
    }

    /**
     * @return array<array<string>>
     */
    public static function provider_reserved_characters_values(): array
    {
        return [
            ['nam|e'],
            ['nam/e'],
            ['nam\e'],
            ['nam:e'],
            ['nam>e'],
            ['nam<e'],
            ['nam*e'],
            ['nam?e'],
            ['nam{e'],
            ['nam}e'],
            ['nam=e'],
            ['nam e'],
            ['zażółć gęślą jaźń'],
        ];
    }

    public static function float_values(): Generator
    {
        yield 'shortest round trip' => [0.1 + 0.2, '0.30000000000000004'];
        yield 'small' => [1.0E-9, '1.0e-9'];
        yield 'zero fraction' => [1.0, '1.0'];
        yield 'infinity' => [INF, 'INF'];
        yield 'negative infinity' => [-INF, '-INF'];
        yield 'not a number' => [NAN, 'NAN'];
    }

    public static function round_trips(): Generator
    {
        yield 'datetime' => [type_datetime(), new DateTimeImmutable('2026-10-02 13:00:00 UTC')];
        yield 'datetime in a column zone' => [
            type_datetime('America/Los_Angeles'),
            new DateTimeImmutable('2023-01-01 21:51:14 PST'),
        ];
        yield 'datetime microseconds' => [type_datetime(), new DateTimeImmutable('2026-10-02 13:00:00.123456 UTC')];
        yield 'first 02:30 of a DST fall-back' => [
            type_datetime('Europe/Warsaw'),
            new DateTimeImmutable('2026-10-25 00:30:00 UTC'),
        ];
        yield 'second 02:30 of a DST fall-back' => [
            type_datetime('Europe/Warsaw'),
            new DateTimeImmutable('2026-10-25 01:30:00 UTC'),
        ];
        yield 'float' => [type_float(), 0.1 + 0.2];
        yield 'small float' => [type_float(), 1.0E-9];
        yield 'date' => [type_date(), new DateTimeImmutable('2026-10-02')];
        yield 'integer' => [type_integer(), -42];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('round_trips')]
    public function test_a_partition_value_casts_back_to_the_written_value(Type $type, mixed $value): void
    {
        $segment = (new Partition('c', Partition::fromValue('c', $type, $value)))->segment();

        static::assertEquals($value, $type->cast(Partition::fromSegment($segment)?->value));
    }

    public function test_a_float_partition_value_ignores_a_lossy_serialize_precision(): void
    {
        $previous = ini_get('serialize_precision');
        ini_set('serialize_precision', '10');

        try {
            static::assertSame('0.30000000000000004', Partition::fromValue('x', type_float(), 0.1 + 0.2));
            static::assertSame('10', ini_get('serialize_precision'));
        } finally {
            ini_set('serialize_precision', (string) $previous);
        }
    }

    public function test_creating_partition_value_from_date(): void
    {
        static::assertEquals('2023-01-01T00:00:00+00:00', Partition::fromValue(
            'date',
            type_datetime(),
            new DateTimeImmutable('2023-01-01 00:00:00 UTC'),
        ));
    }

    public function test_creating_partition_value_from_datetime(): void
    {
        static::assertEquals('2023-01-02T05:51:14+00:00', Partition::fromValue(
            'date',
            type_datetime(),
            new DateTimeImmutable('2023-01-01 21:51:14 PST'),
        ));
    }

    public function test_partition_value_carries_the_column_zone_offset(): void
    {
        static::assertEquals('2023-01-01T21:51:14-08:00', Partition::fromValue(
            'date',
            type_datetime('America/Los_Angeles'),
            new DateTimeImmutable('2023-01-01 21:51:14 PST'),
        ));
    }

    public function test_partition_value_keeps_microseconds(): void
    {
        static::assertSame('2026-10-02T13:00:00.123456+00:00', Partition::fromValue(
            'date',
            type_datetime(),
            new DateTimeImmutable('2026-10-02 13:00:00.123456 UTC'),
        ));
    }

    #[DataProvider('float_values')]
    public function test_creating_partition_value_from_float(float $value, string $expected): void
    {
        static::assertSame($expected, Partition::fromValue('x', type_float(), $value));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_creating_partition_value_from_html(): void
    {
        $this->expectExceptionMessage('Column "html" of type html can\'t be used as a partition');

        Partition::fromValue('html', type_html(), '<!DOCTYPE html><html><head></head><body></body></html>');
    }

    public function test_creating_partition_value_from_xml(): void
    {
        $this->expectExceptionMessage('Column "xml" of type xml can\'t be used as a partition');

        Partition::fromValue('xml', type_xml(), '<xml></xml>');
    }

    public function test_creating_partitions_from_uri_decodes_an_encoded_value(): void
    {
        $partitions = Partition::fromUri('/dataset/country=U%7CS/something');

        static::assertCount(1, $partitions);
        static::assertEquals(new Partition('country', 'U|S'), $partitions[0]);
    }

    public function test_a_null_value_is_written_and_read_back_as_the_hive_sentinel(): void
    {
        static::assertSame('region=__HIVE_DEFAULT_PARTITION__', (new Partition('region', null))->segment());
        static::assertEquals(
            new Partition('region', null),
            Partition::fromSegment('region=__HIVE_DEFAULT_PARTITION__'),
        );
    }

    public function test_an_empty_value_is_written_and_read_back_apart_from_null(): void
    {
        static::assertSame('region=', (new Partition('region', ''))->segment());
        static::assertSame('region', Partition::fromSegment('region=')?->name);
        static::assertSame('', Partition::fromSegment('region=')?->value);
    }

    public function test_creating_partitions_from_uri_reads_an_empty_value(): void
    {
        static::assertSame(
            ['country' => '', 'year' => '2026'],
            array_column(
                array_map(static fn(Partition $p): array => [
                    'name' => $p->name,
                    'value' => $p->value,
                ], Partition::fromUri('/dataset/country=/year=2026/file.parquet')->toArray()),
                'value',
                'name',
            ),
        );
    }

    public function test_a_segment_without_a_name_is_not_a_partition(): void
    {
        static::assertNull(Partition::fromSegment('=value'));
    }

    public function test_an_empty_name_is_still_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Partition name can't be empty");

        new Partition('', 'value');
    }

    public function test_a_segment_that_is_not_a_pair_is_not_a_partition(): void
    {
        static::assertNull(Partition::fromSegment('something'));
    }

    public function test_creating_partitions_from_uri_with_partitions(): void
    {
        $partitions = Partition::fromUri('/dataset/country=US/age-range=20-45');

        static::assertCount(2, $partitions);
        static::assertEquals(
            [
                new Partition('country', 'US'),
                new Partition('age-range', '20-45'),
            ],
            $partitions->toArray(),
        );
    }

    public function test_creating_partitions_from_uri_without_partitions(): void
    {
        $partitions = Partition::fromUri('/some/regular/uri/to/file.csv');

        static::assertCount(0, $partitions);
    }

    #[DataProvider('provider_reserved_characters_values')]
    public function test_a_reserved_character_in_the_name_survives_the_path(string $value): void
    {
        static::assertEquals(
            new Partition($value, 'value'),
            Partition::fromSegment((new Partition($value, 'value'))->segment()),
        );
    }

    #[DataProvider('provider_reserved_characters_values')]
    public function test_a_reserved_character_in_the_value_survives_the_path(string $value): void
    {
        static::assertEquals(
            new Partition('name', $value),
            Partition::fromSegment((new Partition('name', $value))->segment()),
        );
    }
}
