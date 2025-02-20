<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Transformation;

use function Flow\ETL\DSL\{df, from_array};
use Flow\Bridge\Symfony\HttpFoundation\Transformation\AddRowIndex;
use Flow\Bridge\Symfony\HttpFoundation\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Tests\FlowTestCase;

final class AddRowIndexTest extends FlowTestCase
{
    public function test_adding_row_index_to_each_row() : void
    {
        $rows = df()
            ->read(from_array(
                [
                    ['id' => 1, 'name' => 'John Doe', 'salary' => 7000, 'currency' => 'USD'],
                    ['id' => 2, 'name' => 'Jane Doe', 'salary' => 8000, 'currency' => 'USD'],
                    ['id' => 3, 'name' => 'John Smith', 'salary' => 9000, 'currency' => 'USD'],
                    ['id' => 4, 'name' => 'Jane Smith', 'salary' => 10000, 'currency' => 'USD'],
                ]
            ))
            ->with(new AddRowIndex())
            ->fetch()
            ->toArray();

        self::assertEquals(
            [
                ['index' => 0, 'id' => 1, 'name' => 'John Doe', 'salary' => 7000, 'currency' => 'USD'],
                ['index' => 1, 'id' => 2, 'name' => 'Jane Doe', 'salary' => 8000, 'currency' => 'USD'],
                ['index' => 2, 'id' => 3, 'name' => 'John Smith', 'salary' => 9000, 'currency' => 'USD'],
                ['index' => 3, 'id' => 4, 'name' => 'Jane Smith', 'salary' => 10000, 'currency' => 'USD'],
            ],
            $rows
        );
    }

    public function test_adding_row_index_to_each_row_starting_from_1() : void
    {
        $rows = df()
            ->read(from_array(
                [
                    ['id' => 1, 'name' => 'John Doe', 'salary' => 7000, 'currency' => 'USD'],
                    ['id' => 2, 'name' => 'Jane Doe', 'salary' => 8000, 'currency' => 'USD'],
                    ['id' => 3, 'name' => 'John Smith', 'salary' => 9000, 'currency' => 'USD'],
                    ['id' => 4, 'name' => 'Jane Smith', 'salary' => 10000, 'currency' => 'USD'],
                ]
            ))
            ->with(new AddRowIndex(startFrom: StartFrom::ONE))
            ->fetch()
            ->toArray();

        self::assertEquals(
            [
                ['index' => 1, 'id' => 1, 'name' => 'John Doe', 'salary' => 7000, 'currency' => 'USD'],
                ['index' => 2, 'id' => 2, 'name' => 'Jane Doe', 'salary' => 8000, 'currency' => 'USD'],
                ['index' => 3, 'id' => 3, 'name' => 'John Smith', 'salary' => 9000, 'currency' => 'USD'],
                ['index' => 4, 'id' => 4, 'name' => 'Jane Smith', 'salary' => 10000, 'currency' => 'USD'],
            ],
            $rows
        );
    }
}
