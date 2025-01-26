<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{config, flow_context};
use function Flow\ETL\DSL\{int_entry, row, rows, str_entry};
use Flow\ETL\Join\Expression;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\{DataFrame, DataFrameFactory, Rows, Tests\FlowTestCase};

final class JoinEachRowsTransformerTest extends FlowTestCase
{
    public function test_inner_join_rows() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $right = new class implements DataFrameFactory {
            public function from(Rows $rows) : DataFrame
            {
                return data_frame()->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                        row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
                    )
                );
            }
        };

        $transformer = JoinEachRowsTransformer::inner($right, Expression::on(['country' => 'code'], 'joined_'));

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
            ],
            $transformer->transform($left, flow_context(config()))->toArray()
        );
    }

    public function test_left_join_rows() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );
        $right = new class implements DataFrameFactory {
            public function from(Rows $rows) : DataFrame
            {
                return data_frame()->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                        row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
                    )
                );
            }
        };

        $transformer = JoinEachRowsTransformer::left($right, Expression::on(['country' => 'code'], 'joined_'));

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 3, 'country' => 'FR', 'joined_name' => null, 'joined_code' => null],
            ],
            $transformer->transform($left, flow_context(config()))->toArray()
        );
    }

    public function test_right_join_rows() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );
        $right = new class implements DataFrameFactory {
            public function from(Rows $rows) : DataFrame
            {
                return data_frame()->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                        row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
                    )
                );
            }
        };

        $transformer = JoinEachRowsTransformer::right($right, Expression::on(['country' => 'code'], 'joined_'));

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => null, 'country' => null, 'joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            $transformer->transform($left, flow_context(config()))->toArray()
        );
    }
}
