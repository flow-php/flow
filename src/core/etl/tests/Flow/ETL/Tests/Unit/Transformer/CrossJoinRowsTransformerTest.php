<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CrossJoinRowsTransformerTest extends FlowTestCase
{
    public function test_bind_concatenates_the_left_and_the_right_schema(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new CrossJoinRowsTransformer(df()->read(from_rows(rows(
                schema(str_schema('name')),
                row(['name' => 'Alice']),
            )))))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_bind_prefixes_every_right_side_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('right_name')),
            (new CrossJoinRowsTransformer(
                df()->read(from_rows(rows(schema(str_schema('name')), row(['name' => 'Alice'])))),
                'right_',
            ))->bind(schema(int_schema('id')))->output,
        );
    }
}
