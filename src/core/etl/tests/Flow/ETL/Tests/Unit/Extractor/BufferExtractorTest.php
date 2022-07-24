<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class BufferExtractorTest extends TestCase
{
    public function test_buffer_extractor_from_single_row_rows_even() : void
    {
        $extractor = From::buffer(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(Row::create(Entry::integer('id', 1)));
                    yield new Rows(Row::create(Entry::integer('id', 2)));
                    yield new Rows(Row::create(Entry::integer('id', 3)));
                    yield new Rows(Row::create(Entry::integer('id', 4)));
                    yield new Rows(Row::create(Entry::integer('id', 5)));
                    yield new Rows(Row::create(Entry::integer('id', 6)));
                    yield new Rows(Row::create(Entry::integer('id', 7)));
                    yield new Rows(Row::create(Entry::integer('id', 8)));
                }
            },
            2
        );

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 3)),
                    Row::create(Entry::integer('id', 4))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 5)),
                    Row::create(Entry::integer('id', 6))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 7)),
                    Row::create(Entry::integer('id', 8))
                ),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_buffer_extractor_from_single_row_rows_odd() : void
    {
        $extractor = From::buffer(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(Row::create(Entry::integer('id', 1)));
                    yield new Rows(Row::create(Entry::integer('id', 2)));
                    yield new Rows(Row::create(Entry::integer('id', 3)));
                    yield new Rows(Row::create(Entry::integer('id', 4)));
                    yield new Rows(Row::create(Entry::integer('id', 5)));
                    yield new Rows(Row::create(Entry::integer('id', 6)));
                    yield new Rows(Row::create(Entry::integer('id', 7)));
                }
            },
            2
        );

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 3)),
                    Row::create(Entry::integer('id', 4))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 5)),
                    Row::create(Entry::integer('id', 6))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 7)),
                ),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_buffer_extractor_from_single_row_rows_odd_max_size_odd() : void
    {
        $extractor = From::buffer(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(Row::create(Entry::integer('id', 1)));
                    yield new Rows(Row::create(Entry::integer('id', 2)));
                    yield new Rows(Row::create(Entry::integer('id', 3)));
                    yield new Rows(Row::create(Entry::integer('id', 4)));
                    yield new Rows(Row::create(Entry::integer('id', 5)));
                    yield new Rows(Row::create(Entry::integer('id', 6)));
                    yield new Rows(Row::create(Entry::integer('id', 7)));
                }
            },
            3
        );

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                    Row::create(Entry::integer('id', 3)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 4)),
                    Row::create(Entry::integer('id', 5)),
                    Row::create(Entry::integer('id', 6))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 7)),
                ),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_buffer_extractor_when_equals_to_max_rows_size() : void
    {
        $extractor = From::buffer(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(
                        Row::create(Entry::integer('id', 1)),
                        Row::create(Entry::integer('id', 2)),
                    );
                }
            },
            2
        );

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                ),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_buffer_extractor_when_less_than_max_rows_size() : void
    {
        $extractor = From::buffer(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(
                        Row::create(Entry::integer('id', 1)),
                        Row::create(Entry::integer('id', 2)),
                    );
                }
            },
            10
        );

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                ),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_buffer_extractor_when_more_even_than_max_rows_size() : void
    {
        $extractor = From::buffer(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(
                        Row::create(Entry::integer('id', 1)),
                        Row::create(Entry::integer('id', 2)),
                        Row::create(Entry::integer('id', 3)),
                        Row::create(Entry::integer('id', 4)),
                    );

                    yield new Rows(
                        Row::create(Entry::integer('id', 5)),
                        Row::create(Entry::integer('id', 6)),
                        Row::create(Entry::integer('id', 7)),
                        Row::create(Entry::integer('id', 8)),
                    );
                }
            },
            2
        );

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 3)),
                    Row::create(Entry::integer('id', 4)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 5)),
                    Row::create(Entry::integer('id', 6)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 7)),
                    Row::create(Entry::integer('id', 8)),
                ),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_buffer_extractor_when_more_odd_than_max_rows_size() : void
    {
        $extractor = From::buffer(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(
                        Row::create(Entry::integer('id', 1)),
                        Row::create(Entry::integer('id', 2)),
                        Row::create(Entry::integer('id', 3)),
                        Row::create(Entry::integer('id', 4)),
                    );

                    yield new Rows(
                        Row::create(Entry::integer('id', 5)),
                        Row::create(Entry::integer('id', 6)),
                        Row::create(Entry::integer('id', 7)),
                    );
                }
            },
            2
        );

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 3)),
                    Row::create(Entry::integer('id', 4)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 5)),
                    Row::create(Entry::integer('id', 6)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 7)),
                ),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }
}
