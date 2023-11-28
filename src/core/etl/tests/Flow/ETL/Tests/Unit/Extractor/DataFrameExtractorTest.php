<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\FlowContext;
use PHPUnit\Framework\TestCase;

final class DataFrameExtractorTest extends TestCase
{
    public function test_extracting_from_another_data_frame() : void
    {
        $this->assertEquals(
            [
                rows(
                    row(Entry::str('value', 'test')),
                    row(Entry::str('value', 'test')),
                ),
                rows(
                    row(Entry::str('value', 'test')),
                    row(Entry::str('value', 'test')),
                ),
            ],
            \iterator_to_array(
                from_data_frame(
                    read(
                        from_rows(
                            rows(
                                row(Entry::str('value', 'test')),
                                row(Entry::str('value', 'test')),
                            ),
                            rows(
                                row(Entry::str('value', 'test')),
                                row(Entry::str('value', 'test')),
                            )
                        )
                    ),
                )->extract(new FlowContext(Config::default()))
            ),
        );
    }
}
