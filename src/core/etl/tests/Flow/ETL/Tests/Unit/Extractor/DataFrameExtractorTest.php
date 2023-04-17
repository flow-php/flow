<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\Extractor\DataFrameExtractor;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class DataFrameExtractorTest extends TestCase
{
    public function test_extracting_from_another_data_frame() : void
    {
        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::str('value', 'test')),
                    Row::create(Entry::str('value', 'test')),
                ),
                new Rows(
                    Row::create(Entry::str('value', 'test')),
                    Row::create(Entry::str('value', 'test')),
                ),
            ],
            \iterator_to_array(
                (new DataFrameExtractor(
                    (new Flow())
                        ->extract(From::rows(
                            new Rows(
                                Row::create(Entry::str('value', 'test')),
                                Row::create(Entry::str('value', 'test')),
                            ),
                            new Rows(
                                Row::create(Entry::str('value', 'test')),
                                Row::create(Entry::str('value', 'test')),
                            )
                        )),
                ))->extract(new FlowContext(Config::default()))
            ),
        );
    }
}
