<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{df, from_data_frame, from_rows, row, rows, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class DataFrameExtractorTest extends FlowTestCase
{
    public function test_extracting_from_another_data_frame() : void
    {
        $extractor = from_data_frame(
            df()->read(from_rows(
                rows(
                    row(str_entry('value', 'test')),
                    row(str_entry('value', 'test')),
                ),
                rows(
                    row(str_entry('value', 'test')),
                    row(str_entry('value', 'test')),
                )
            ))
        );

        self::assertExtractedRowsEquals(
            rows(
                row(str_entry('value', 'test')),
                row(str_entry('value', 'test')),
                row(str_entry('value', 'test')),
                row(str_entry('value', 'test')),
            ),
            $extractor
        );
    }
}
