<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\JoinEachRowsTransformer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinEachRowsTransformerTest extends FlowTestCase
{
    public function test_bind_refuses_to_derive_a_data_dependent_schema(): void
    {
        $right = new class implements DataFrameFactory {
            public function from(Rows $rows): DataFrame
            {
                return data_frame()->process($rows);
            }
        };

        $this->expectException(DataDependentSchemaException::class);
        $this->expectExceptionMessage(
            "cannot describe its output before rows flow: its right side is built from each left batch's row values",
        );

        JoinEachRowsTransformer::inner($right, Expression::on(['id' => 'id']))->bind(schema(int_schema('id')));
    }

    public function test_inner_join_rows(): void
    {
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );

        $right = new class implements DataFrameFactory {
            public function from(Rows $rows): DataFrame
            {
                return data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ));
            }
        };

        $transformer = JoinEachRowsTransformer::inner($right, Expression::on(['country' => 'code'], 'joined_'));

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
            ],
            $transformer->transform($left, flow_context(config()))->toArray(),
        );
    }

    public function test_left_join_rows(): void
    {
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );
        $right = new class implements DataFrameFactory {
            public function from(Rows $rows): DataFrame
            {
                return data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ));
            }
        };

        $transformer = JoinEachRowsTransformer::left($right, Expression::on(['country' => 'code'], 'joined_'));

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 3, 'country' => 'FR', 'joined_name' => null, 'joined_code' => null],
            ],
            $transformer->transform($left, flow_context(config()))->toArray(),
        );
    }

    public function test_right_join_rows(): void
    {
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );
        $right = new class implements DataFrameFactory {
            public function from(Rows $rows): DataFrame
            {
                return data_frame()->process(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ));
            }
        };

        $transformer = JoinEachRowsTransformer::right($right, Expression::on(['country' => 'code'], 'joined_'));

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => null, 'country' => null, 'joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            $transformer->transform($left, flow_context(config()))->toArray(),
        );
    }
}
