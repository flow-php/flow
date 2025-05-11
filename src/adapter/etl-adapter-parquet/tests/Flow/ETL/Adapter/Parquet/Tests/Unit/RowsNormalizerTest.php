<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\ETL\DSL\{row, rows, schema, str_entry, str_schema};
use Flow\ETL\Adapter\Parquet\RowsNormalizer;
use Flow\ETL\Tests\FlowTestCase;

final class RowsNormalizerTest extends FlowTestCase
{
    public function test_normalization_nullable_entry() : void
    {
        $rows = rows(row(str_entry('id', null)));
        $schema = schema(str_schema('id'));

        self::assertEquals(
            [
                [
                    'id' => '',
                ],
            ],
            (new RowsNormalizer())->normalize($rows, $schema)
        );
    }
}
