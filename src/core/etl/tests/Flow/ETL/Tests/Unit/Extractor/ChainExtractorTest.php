<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{from_all, int_entry};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\{Extractor, FlowContext, Tests\FlowTestCase};

final class ChainExtractorTest extends FlowTestCase
{
    public function test_chain_extractor() : void
    {
        $extractor = from_all(
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield rows(row(int_entry('id', 1)));
                    yield rows(row(int_entry('id', 2)));
                }
            },
            new class implements Extractor {
                public function extract(FlowContext $context) : \Generator
                {
                    yield rows(row(int_entry('id', 3)));
                    yield rows(row(int_entry('id', 4)));
                }
            },
        );

        self::assertExtractedRowsEquals(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3)), row(int_entry('id', 4))),
            $extractor
        );
    }
}
