<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use DateTimeImmutable;
use Flow\ETL\Loader\RowPartitions;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class RowPartitionsTest extends FlowTestCase
{
    /**
     * @return array<string, array{Definition<mixed>, mixed, string}>
     */
    public static function scalar_columns(): array
    {
        return [
            'int' => [int_schema('v'), 7, '7'],
            'float' => [float_schema('v'), 1.5, '1.500000'],
            'string' => [str_schema('v'), 'eu', 'eu'],
            'bool' => [bool_schema('v'), true, 'true'],
            'datetime truncates to a day' => [
                datetime_schema('v'),
                new DateTimeImmutable('2024-01-01 21:30:00'),
                '2024-01-01',
            ],
        ];
    }

    #[DataProvider('scalar_columns')]
    public function test_a_scalar_column_becomes_its_formatted_value(
        Definition $definition,
        mixed $value,
        string $expected,
    ): void {
        static::assertSame(
            $expected,
            (new RowPartitions(refs(ref('v'))))->of(array_to_rows([['v' => $value]], schema($definition)), 0)->get(
                'v',
            )->value,
        );
    }

    public function test_an_aliased_partition_reference_reads_its_base_column(): void
    {
        static::assertSame(
            '7',
            (new RowPartitions(refs(ref('v')->as('alias'))))->of(
                array_to_rows([['v' => 7, 'alias' => 9]], schema(int_schema('v'), int_schema('alias'))),
                0,
            )->get('alias')->value,
        );
    }

    public function test_a_null_value_stays_null(): void
    {
        static::assertNull((new RowPartitions(refs(ref('v'))))->of(array_to_rows([[
            'v' => null,
        ]], schema(str_schema('v', nullable: true))), 0)->get('v')->value);
    }

    public function test_a_list_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('can\'t be used as a partition');

        (new RowPartitions(refs(ref('v'))))->of(array_to_rows([['v' => [
            1,
            2,
        ]]], schema(list_schema('v', type_list(type_integer())))), 0);
    }

    public function test_a_structure_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('can\'t be used as a partition');

        $structure = type_structure(['a' => type_string()]);

        (new RowPartitions(refs(ref('v'))))->of(array_to_rows([['v' => $structure->cast([
            'a' => 'x',
        ])]], schema(structure_schema('v', $structure))), 0);
    }
}
