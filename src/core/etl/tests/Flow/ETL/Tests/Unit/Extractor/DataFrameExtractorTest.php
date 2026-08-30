<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DataFrameExtractorTest extends FlowTestCase
{
    public function test_extracting_from_another_data_frame(): void
    {
        $extractor = from_data_frame(df()->read(from_rows(
            rows(schema(str_schema('value')), row(['value' => 'test']), row(['value' => 'test'])),
            rows(schema(str_schema('value')), row(['value' => 'test']), row(['value' => 'test'])),
        )));

        self::assertExtractedRowsEquals(
            rows(
                schema(str_schema('value')),
                row(['value' => 'test']),
                row(['value' => 'test']),
                row(['value' => 'test']),
                row(['value' => 'test']),
            ),
            $extractor,
        );
    }
}
