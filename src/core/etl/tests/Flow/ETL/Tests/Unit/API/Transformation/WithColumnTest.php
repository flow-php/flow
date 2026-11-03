<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;
use function Flow\ETL\DSL\with_column;

final class WithColumnTest extends FlowTestCase
{
    public function test_with_column_through_invoke_with_a_column_name(): void
    {
        $dataFrame = df()->read(from_array([['a' => 1], ['a' => 2]]));
        $expected = [['a' => 1, 'b' => 2], ['a' => 2, 'b' => 3]];

        static::assertSame($expected, with_column('b', ref('a')->plus(lit(1)))($dataFrame)->fetch()->toArray());
        static::assertSame(
            $expected,
            $dataFrame
                ->withColumn('b', ref('a')->plus(lit(1)))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_with_column_through_invoke_with_a_definition(): void
    {
        $dataFrame = df()->read(from_array([['a' => 1], ['a' => 2]]));
        $expected = [['a' => 1, 'x' => 1], ['a' => 2, 'x' => 1]];

        static::assertSame($expected, with_column(int_schema('x'), lit(1))($dataFrame)->fetch()->toArray());
        static::assertSame($expected, $dataFrame->withColumn(int_schema('x'), lit(1))->fetch()->toArray());
        static::assertEquals(int_schema('x'), with_column(int_schema('x'), lit(1))($dataFrame)->schema()->get('x'));
    }

    public function test_with_column_through_invoke_with_a_window_function(): void
    {
        $dataFrame = df()->read(from_array([['a' => 2], ['a' => 1]]));
        $expected = [['a' => 1, 'rank' => 1], ['a' => 2, 'rank' => 2]];

        static::assertSame(
            $expected,
            with_column('rank', rank()->over(window()->orderBy(ref('a'))))($dataFrame)->fetch()->toArray(),
        );
        static::assertSame(
            $expected,
            $dataFrame
                ->withColumn('rank', rank()->over(window()->orderBy(ref('a'))))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_with_column_through_with(): void
    {
        static::assertSame(
            [['a' => 1, 'b' => 2]],
            df()
                ->read(from_array([['a' => 1]]))
                ->with(with_column('b', ref('a')->plus(lit(1))))
                ->fetch()
                ->toArray(),
        );
    }
}
