<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{df, from_data_frame, from_rows, row, rows, str_entry};
use Flow\ETL\{Config, FlowContext};
use PHPUnit\Framework\TestCase;

final class DataFrameExtractorTest extends TestCase
{
    public function test_extracting_from_another_data_frame() : void
    {
        self::assertEquals(
            [
                rows(
                    row(str_entry('value', 'test')),
                    row(str_entry('value', 'test')),
                ),
                rows(
                    row(str_entry('value', 'test')),
                    row(str_entry('value', 'test')),
                ),
            ],
            \iterator_to_array(
                from_data_frame(
                    df()->read(from_rows(
                        rows(
                            row(str_entry('value', 'test')),
                            row(str_entry('value', 'test')),
                        ),
                        rows(
                            row(str_entry('value', 'test')),
                            row(str_entry('value', 'test')),
                        )
                    )),
                )->extract(new FlowContext(Config::default()))
            ),
        );
    }
}
