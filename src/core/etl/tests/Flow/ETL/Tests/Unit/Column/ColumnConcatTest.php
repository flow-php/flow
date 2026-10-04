<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use Flow\ETL\Column\Php\ConstantColumn;
use Flow\ETL\Column\Php\ScalarColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ColumnConcatTest extends TestCase
{
    public function test_scalar_columns(): void
    {
        $column = ColumnMother::of(int_schema('a', nullable: true), [1, null])->concat(ColumnMother::of(
            int_schema('a', nullable: true),
            [null, 4],
        ));

        static::assertSame([1, null, null, 4], $column->values());
        static::assertSame(2, $column->nullCount());
    }

    public function test_list_columns_rebase_offsets_and_rekey_nulls(): void
    {
        $definition = list_schema('a', type_list(type_integer()), nullable: true);
        $column = ColumnMother::of($definition, [[1], null])->concat(ColumnMother::of($definition, [null, [2, 3]]));

        static::assertSame([[1], null, null, [2, 3]], $column->values());
        static::assertSame(2, $column->nullCount());
        static::assertTrue($column->isNull(2));
    }

    public function test_map_columns_rebase_offsets_and_rekey_nulls(): void
    {
        $definition = map_schema('a', type_map(type_string(), type_integer()), nullable: true);
        $column = ColumnMother::of($definition, [['x' => 1], null])->concat(ColumnMother::of($definition, [
            null,
            ['y' => 2, 'z' => 3],
        ]));

        static::assertSame([['x' => 1], null, null, ['y' => 2, 'z' => 3]], $column->values());
        static::assertTrue($column->isNull(2));
    }

    public function test_structure_columns(): void
    {
        $definition = structure_schema('a', type_structure(['x' => type_integer()]), nullable: true);
        $column = ColumnMother::of($definition, [['x' => 1]])->concat(ColumnMother::of($definition, [
            null,
            ['x' => 2],
        ]));

        static::assertSame([['x' => 1], null, ['x' => 2]], $column->values());
        static::assertTrue($column->isNull(1));
    }

    public function test_constant_columns_with_the_same_value_collapse(): void
    {
        $backend = new PhpBackend();
        $column = $backend->constant(int_schema('a'), 7, 2)->concat($backend->constant(int_schema('a'), 7, 3));

        static::assertInstanceOf(ConstantColumn::class, $column);
        static::assertSame(5, $column->count());
    }

    public function test_constant_columns_with_different_values_expand(): void
    {
        $backend = new PhpBackend();
        $column = $backend->constant(int_schema('a'), 7, 2)->concat($backend->constant(int_schema('a'), 8, 1));

        static::assertInstanceOf(ScalarColumn::class, $column);
        static::assertSame([7, 7, 8], $column->values());
    }

    public function test_a_mixed_class_concat_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('cannot concat');

        ColumnMother::of(int_schema('a'), [1])->concat((new PhpBackend())->constant(int_schema('a'), 7, 2));
    }

    public function test_container_columns_refuse_another_column_class(): void
    {
        $constant = (new PhpBackend())->constant(int_schema('a'), 1, 1);
        $containers = [
            ColumnMother::of(list_schema('a', type_list(type_integer())), [[1]]),
            ColumnMother::of(map_schema('a', type_map(type_string(), type_integer())), [['x' => 1]]),
            ColumnMother::of(structure_schema('a', type_structure(['x' => type_integer()])), [['x' => 1]]),
        ];

        foreach ($containers as $container) {
            try {
                $container->concat($constant);
                static::fail($container::class . ' concatenated another column class');
            } catch (InvalidArgumentException $e) {
                static::assertStringContainsString('cannot concat', $e->getMessage());
            }
        }
    }
}
