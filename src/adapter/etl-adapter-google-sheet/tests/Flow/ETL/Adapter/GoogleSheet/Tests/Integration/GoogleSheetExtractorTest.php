<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\Adapter\GoogleSheet\{Columns, GoogleSheetExtractor, Tests\GoogleSheetsContext};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    private GoogleSheetsContext $context;

    protected function setUp() : void
    {
        $this->context = new GoogleSheetsContext();
    }

    public function test_extract_with_cut_extra_columns() : void
    {
        $extractor = new GoogleSheetExtractor(
            $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
            '1234567890',
            new Columns('Sheet', 'A', 'Z'),
        );

        $rows = $extractor->extract(flow_context(config()));

        foreach ($rows as $row) {
            self::assertNotNull($row);
        }
    }

    public function test_extract_without_cut_extra_columns() : void
    {
        $extractor = new GoogleSheetExtractor(
            $this->context->sheets(__DIR__ . '/../Fixtures/extra-columns.json'),
            '1234567890',
            new Columns('Sheet', 'A', 'Z'),
        );
        $extractor->withDropExtraColumns(false);

        $rows = $extractor->extract(flow_context(config()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (4) than headers (3)');

        foreach ($rows as $row) {
            self::assertNotNull($row);
        }
    }
}
