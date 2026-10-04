<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class CastingColumnBuilderTest extends TestCase
{
    public function test_casts_every_value(): void
    {
        $builder = (new PhpBackend())->builder(int_schema('a', nullable: true));
        $builder->append('1');
        $builder->appendMany(['2', null]);

        static::assertSame(3, $builder->count());
        static::assertSame([1, 2, null], $builder->finish()->physicals());
    }

    public function test_append_many_is_atomic_on_a_refusal(): void
    {
        $builder = (new PhpBackend())->builder(int_schema('a'));

        try {
            $builder->appendMany(['1', 'x']);
        } catch (SchemaMismatchException) {
        }

        static::assertSame(0, $builder->count());
    }

    public function test_append_many_refuses_null_under_not_null(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "a" (row 1): could not convert null to integer, column is not nullable');

        (new PhpBackend())
            ->builder(int_schema('a'))
            ->appendMany([1, null]);
    }

    public function test_the_null_definition_accepts_null(): void
    {
        $builder = (new PhpBackend())->builder(null_schema('a'));
        $builder->append(null);
        $builder->appendMany([null]);

        static::assertSame(2, $builder->count());
    }

    public function test_append_from_copies_a_physical_cell(): void
    {
        $source = ColumnMother::of(int_schema('a', nullable: true), [1, null, 3]);
        $builder = (new PhpBackend())->builder(int_schema('a', nullable: true));
        $builder->appendFrom($source, 2);
        $builder->appendFrom($source, 1);
        $builder->appendTake($source, [0, 1, 2]);

        static::assertSame([3, null, 1, null, 3], $builder->finish()->physicals());
    }

    public function test_append_from_refuses_null_under_not_null(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('could not convert null to integer, column is not nullable');

        (new PhpBackend())
            ->builder(int_schema('a'))
            ->appendFrom(ColumnMother::of(int_schema('a', nullable: true), [null]), 0);
    }

    public function test_append_take_refuses_null_under_not_null(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('could not convert null to integer, column is not nullable');

        (new PhpBackend())
            ->builder(int_schema('a'))
            ->appendTake(ColumnMother::of(int_schema('a', nullable: true), [1, null]), [0, 1]);
    }

    #[TestWith([2])]
    #[TestWith([null])]
    public function test_append_physicals_takes_physicals_with_nulls(?int $nullCount): void
    {
        $builder = (new PhpBackend())->builder(int_schema('id', nullable: true));
        $builder->appendPhysicals([1, null, 3, null], $nullCount);
        $column = $builder->finish();

        static::assertSame([1, null, 3, null], $column->values());
        static::assertSame(2, $column->nullCount());
    }

    public function test_append_physicals_refuses_null_under_not_null_at_its_position(): void
    {
        $builder = (new PhpBackend())->builder(int_schema('id'));

        try {
            $builder->appendPhysicals([1, 2, null]);
            static::fail('a null under NOT NULL must be refused');
        } catch (SchemaMismatchException $e) {
            static::assertSame(2, $e->rowIndex);
        }

        static::assertSame(0, $builder->count());
    }

    public function test_append_physicals_of_nothing_appends_nothing(): void
    {
        $builder = (new PhpBackend())->builder(int_schema('id'));
        $builder->appendPhysicals([], 0);

        static::assertSame(0, $builder->finish()->count());
    }

    #[TestWith(['integer', '1'])]
    #[TestWith(['float', 1])]
    #[TestWith(['datetime', 'not micros'])]
    #[TestWith(['uuid', 'too short'])]
    #[TestWith(['list', 'not an array'])]
    #[TestWith(['list_of_int', ['1']])]
    public function test_append_physicals_refuses_a_physical_of_another_kind_and_appends_nothing(
        string $definition,
        mixed $physical,
    ): void {
        $builder = (new PhpBackend())->builder(match ($definition) {
            'integer' => int_schema('c'),
            'float' => float_schema('c'),
            'datetime' => datetime_schema('c'),
            'uuid' => uuid_schema('c'),
            'list' => list_schema('c', type_list(type_string())),
            default => list_schema('c', type_list(type_integer())),
        });

        try {
            $builder->appendPhysicals([$physical]);
            static::fail('a physical of another kind must be refused');
        } catch (InvalidArgumentException $e) {
            static::assertStringContainsString('physical in a', $e->getMessage());
        }

        static::assertSame(0, $builder->count());
    }

    public function test_append_physicals_stores_any_physical_of_a_null_column_as_null(): void
    {
        $builder = (new PhpBackend())->builder(null_schema('c'));
        $builder->appendPhysicals([null]);

        static::assertSame([null], $builder->finish()->physicals());
    }
}
