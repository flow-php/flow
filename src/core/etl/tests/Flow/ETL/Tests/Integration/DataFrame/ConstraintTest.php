<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{constraint_sorted_by, constraint_unique, df, from_array, ref};
use Flow\ETL\Exception\ConstraintViolationException;
use Flow\ETL\Tests\FlowTestCase;

final class ConstraintTest extends FlowTestCase
{
    public function test_sorted_ascending_integers() : void
    {
        $result = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
                ['id' => 3, 'name' => 'Doe'],
                ['id' => 4, 'name' => 'Michael'],
            ]))
            ->constrain(constraint_sorted_by(ref('id')))
            ->fetch();

        self::assertCount(4, $result);
    }

    public function test_sorted_ascending_integers_violation() : void
    {
        $this->expectException(ConstraintViolationException::class);
        $this->expectExceptionMessage('Constraint violation: Sorted constraint on [id ASC]');

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
                ['id' => 5, 'name' => 'Doe'],
                ['id' => 3, 'name' => 'Michael'],
            ]))
            ->constrain(constraint_sorted_by(ref('id')->asc()))
            ->run();
    }

    public function test_sorted_default_order_is_ascending() : void
    {
        $result = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
            ]))
            ->constrain(constraint_sorted_by('id'))
            ->fetch();

        self::assertCount(3, $result);
    }

    public function test_sorted_descending_floats() : void
    {
        $result = df()
            ->read(from_array([
                ['price' => 99.99, 'product' => 'A'],
                ['price' => 49.99, 'product' => 'B'],
                ['price' => 19.99, 'product' => 'C'],
            ]))
            ->constrain(constraint_sorted_by(ref('price')->desc()))
            ->fetch();

        self::assertCount(3, $result);
    }

    public function test_sorted_descending_violation() : void
    {
        $this->expectException(ConstraintViolationException::class);
        $this->expectExceptionMessage('Constraint violation: Sorted constraint on [price DESC]');

        df()
            ->read(from_array([
                ['price' => 99.99, 'product' => 'A'],
                ['price' => 49.99, 'product' => 'B'],
                ['price' => 59.99, 'product' => 'C'],
            ]))
            ->constrain(constraint_sorted_by(ref('price')->desc()))
            ->run();
    }

    public function test_sorted_multiple_columns() : void
    {
        $result = df()
            ->read(from_array([
                ['category' => 'A', 'price' => 100.0, 'name' => 'Product 1'],
                ['category' => 'A', 'price' => 50.0, 'name' => 'Product 2'],
                ['category' => 'B', 'price' => 200.0, 'name' => 'Product 3'],
                ['category' => 'B', 'price' => 150.0, 'name' => 'Product 4'],
            ]))
            ->constrain(constraint_sorted_by(ref('category'), ref('price')->desc()))
            ->fetch();

        self::assertCount(4, $result);
    }

    public function test_sorted_multiple_columns_violation() : void
    {
        $this->expectException(ConstraintViolationException::class);
        $this->expectExceptionMessage('Constraint violation: Sorted constraint on [category ASC, price DESC]');

        df()
            ->read(from_array([
                ['category' => 'A', 'price' => 100.0],
                ['category' => 'A', 'price' => 50.0],
                ['category' => 'B', 'price' => 100.0],
                ['category' => 'B', 'price' => 200.0],
            ]))
            ->constrain(constraint_sorted_by(ref('category')->asc(), ref('price')->desc()))
            ->run();
    }

    public function test_sorted_strings_ascending() : void
    {
        $result = df()
            ->read(from_array([
                ['name' => 'Alice'],
                ['name' => 'Bob'],
                ['name' => 'Charlie'],
                ['name' => 'David'],
            ]))
            ->constrain(constraint_sorted_by('name'))
            ->fetch();

        self::assertCount(4, $result);
    }

    public function test_sorted_with_duplicate_values() : void
    {
        $result = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'A'],
                ['id' => 1, 'name' => 'B'],
                ['id' => 2, 'name' => 'C'],
                ['id' => 2, 'name' => 'D'],
            ]))
            ->constrain(constraint_sorted_by(ref('id')))
            ->fetch();

        self::assertCount(4, $result);
    }

    public function test_sorted_with_nulls_at_beginning_asc() : void
    {
        $result = df()
            ->read(from_array([
                ['id' => null],
                ['id' => 1],
                ['id' => 2],
            ]))
            ->constrain(constraint_sorted_by(ref('id')))
            ->fetch();

        self::assertCount(3, $result);
    }

    public function test_sorted_with_nulls_at_end_asc_violation() : void
    {
        $this->expectException(ConstraintViolationException::class);

        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => null],
            ]))
            ->constrain(constraint_sorted_by(ref('id')))
            ->run();
    }

    public function test_sorted_with_nulls_at_end_desc() : void
    {
        $result = df()
            ->read(from_array([
                ['id' => 2],
                ['id' => 1],
                ['id' => null],
            ]))
            ->constrain(constraint_sorted_by(ref('id')->desc()))
            ->fetch();

        self::assertCount(3, $result);
    }

    public function test_sorted_with_nulls_in_middle_violation() : void
    {
        $this->expectException(ConstraintViolationException::class);

        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => null],
                ['id' => 2],
            ]))
            ->constrain(constraint_sorted_by(ref('id')))
            ->run();
    }

    public function test_unique_on_multiple_fields() : void
    {
        $this->expectException(ConstraintViolationException::class);
        $this->expectExceptionMessage('Constraint violation: Unique constraint on [id, sub_id] - Values: [id<integer> = 4, sub_id<integer> = 4] in row: 5');

        df()
            ->read(from_array([
                ['id' => 1, 'sub_id' => 1, 'name' => 'John'],
                ['id' => 2, 'sub_id' => 1, 'name' => 'Jane'],
                ['id' => 3, 'sub_id' => 1, 'name' => 'Doe'],
                ['id' => 1, 'sub_id' => 2, 'name' => 'John'],
                ['id' => 4, 'sub_id' => 4, 'name' => 'Michael'],
                ['id' => 4, 'sub_id' => 4, 'name' => 'Michael'],
            ]))
            ->constrain(constraint_unique('id', 'sub_id'))
            ->run();
    }
}
