<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use DateTimeImmutable;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowHashes;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class RowHashesTest extends FlowTestCase
{
    public function test_every_row_hashes_like_a_row_of_the_batch(): void
    {
        $schema = schema(
            str_schema('s', nullable: true),
            int_schema('i'),
            float_schema('f'),
            bool_schema('b'),
            datetime_schema('dt'),
            date_schema('d'),
            time_schema('t'),
            uuid_schema('u'),
            json_schema('j'),
            enum_schema('e', BackedStringEnum::class),
            xml_schema('x'),
            time_zone_schema('tz'),
            list_schema('l', type_list(type_integer())),
            map_schema('m', type_map(type_string(), type_integer())),
            structure_schema('st', type_structure(['a' => type_integer()])),
        );
        $rows = array_to_rows([
            [
                's' => 'a',
                'i' => 1,
                'f' => 1.5,
                'b' => true,
                'dt' => new DateTimeImmutable('2024-01-01 10:00:00.5'),
                'd' => new DateTimeImmutable('2024-01-01'),
                't' => 'PT1H',
                'u' => '00000000-0000-4000-8000-000000000001',
                'j' => '{"a":1}',
                'e' => BackedStringEnum::one,
                'x' => '<a>1</a>',
                'tz' => 'UTC',
                'l' => [1, 2],
                'm' => ['k' => 1],
                'st' => ['a' => 1],
            ],
            [
                's' => null,
                'i' => 2,
                'f' => 0.0,
                'b' => false,
                'dt' => new DateTimeImmutable('1969-12-31 23:59:59'),
                'd' => new DateTimeImmutable('1969-12-31'),
                't' => 'PT0S',
                'u' => '00000000-0000-4000-8000-000000000002',
                'j' => '[]',
                'e' => BackedStringEnum::two,
                'x' => '<b/>',
                'tz' => 'Europe/Warsaw',
                'l' => [],
                'm' => [],
                'st' => ['a' => 2],
            ],
        ], $schema);

        $hashes = (new RowHashes())->of($rows);

        static::assertSame(
            [(new RowHashes())->of($rows->slice(0, 1))[0], (new RowHashes())->of($rows->slice(1, 1))[0]],
            $hashes,
        );
        static::assertNotSame($hashes[0], $hashes[1]);
    }

    public function test_a_hash_ignores_column_order(): void
    {
        static::assertSame(
            (new RowHashes())->of(array_to_rows(
                [['id' => 1, 'string' => 'string', 'bool' => false]],
                schema(int_schema('id'), str_schema('string'), bool_schema('bool')),
            )),
            (new RowHashes())->of(array_to_rows(
                [['bool' => false, 'id' => 1, 'string' => 'string']],
                schema(bool_schema('bool'), int_schema('id'), str_schema('string')),
            )),
        );
    }

    public function test_rows_differing_in_element_order_hash_differently(): void
    {
        $hashes = (new RowHashes())->of(array_to_rows([
            ['list' => [1, 2, 3]],
            ['list' => [3, 2, 1]],
        ], schema(list_schema('list', type_list(type_integer())))));

        static::assertNotSame($hashes[0], $hashes[1]);
    }

    public function test_a_row_of_a_batch_without_columns_hashes_the_empty_string(): void
    {
        static::assertSame(
            [(new NativePHPHash())->hash('')],
            (new RowHashes())->of(Rows::fromColumns(schema(), [], 1)),
        );
    }

    public function test_the_algorithm_hashes_each_row(): void
    {
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));

        static::assertSame(
            [(new NativePHPHash('md5'))->hash('id' . NativeHasher::normalize(1))],
            (new RowHashes())->of($rows, new NativePHPHash('md5')),
        );
    }

    public function test_an_empty_batch_has_no_hashes(): void
    {
        static::assertSame([], (new RowHashes())->of(array_to_rows([], schema(int_schema('id')))));
    }
}
