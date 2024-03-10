<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{array_to_rows, flow_context, ref};
use Flow\ETL\Transformer\DropPartitionsTransformer;
use PHPUnit\Framework\TestCase;

final class DropPartitionsTransformerTest extends TestCase
{
    public function test_dropping_partitions() : void
    {
        $partitioned = array_to_rows([
            ['id' => 1, 'name' => 'one', 'category' => 'a'],
            ['id' => 2, 'name' => 'two', 'category' => 'a'],
            ['id' => 3, 'name' => 'three', 'category' => 'a'],
            ['id' => 4, 'name' => 'four', 'category' => 'a'],
            ['id' => 5, 'name' => 'five', 'category' => 'a'],
            ['id' => 6, 'name' => 'six', 'category' => 'b'],
            ['id' => 7, 'name' => 'seven', 'category' => 'b'],
            ['id' => 8, 'name' => 'eight', 'category' => 'b'],
            ['id' => 9, 'name' => 'nine', 'category' => 'b'],
            ['id' => 10, 'name' => 'ten', 'category' => 'b'],
        ])->partitionBy(ref('category'));

        foreach ($partitioned as $rows) {
            self::assertTrue($rows->isPartitioned());

            $notPartitioned = (new DropPartitionsTransformer())->transform($rows, flow_context());

            self::assertFalse($notPartitioned->isPartitioned());
        }
    }

    public function test_dropping_partitions_with_columns() : void
    {
        $partitioned = array_to_rows([
            ['id' => 1, 'name' => 'one', 'category' => 'a'],
            ['id' => 2, 'name' => 'two', 'category' => 'a'],
            ['id' => 3, 'name' => 'three', 'category' => 'a'],
            ['id' => 4, 'name' => 'four', 'category' => 'a'],
            ['id' => 5, 'name' => 'five', 'category' => 'a'],
            ['id' => 6, 'name' => 'six', 'category' => 'b'],
            ['id' => 7, 'name' => 'seven', 'category' => 'b'],
            ['id' => 8, 'name' => 'eight', 'category' => 'b'],
            ['id' => 9, 'name' => 'nine', 'category' => 'b'],
            ['id' => 10, 'name' => 'ten', 'category' => 'b'],
        ])->partitionBy(ref('category'));

        foreach ($partitioned as $rows) {
            self::assertTrue($rows->isPartitioned());

            $notPartitioned = (new DropPartitionsTransformer(true))->transform($rows, flow_context());

            self::assertFalse($notPartitioned->isPartitioned());
            self::assertFalse($notPartitioned->first()->has('category'));
        }
    }

    public function test_transforming_not_partitioned_rows() : void
    {
        $rows = array_to_rows([
            ['id' => 1, 'name' => 'one', 'category' => 'a'],
            ['id' => 2, 'name' => 'two', 'category' => 'a'],
            ['id' => 3, 'name' => 'three', 'category' => 'a'],
            ['id' => 4, 'name' => 'four', 'category' => 'a'],
            ['id' => 5, 'name' => 'five', 'category' => 'a'],
            ['id' => 6, 'name' => 'six', 'category' => 'b'],
            ['id' => 7, 'name' => 'seven', 'category' => 'b'],
            ['id' => 8, 'name' => 'eight', 'category' => 'b'],
            ['id' => 9, 'name' => 'nine', 'category' => 'b'],
            ['id' => 10, 'name' => 'ten', 'category' => 'b'],
        ]);

        self::assertSame(
            $rows,
            (new DropPartitionsTransformer())->transform($rows, flow_context())
        );
    }
}
