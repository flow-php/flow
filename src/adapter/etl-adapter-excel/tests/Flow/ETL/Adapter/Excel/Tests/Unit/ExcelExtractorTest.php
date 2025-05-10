<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

final class ExcelExtractorTest extends FlowTestCase
{
    public function test_invalid_sheet_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sheet name must be a valid Excel sheet name');

        from_excel(__DIR__ . '/../Fixtures/unknown')
            ->withSheetName('This is veeeeeery long excel sheet name, longer than 32 characters');
    }

    public function test_non_local_file() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Only local filesystem paths are supported by ExcelExtractor due to the limitation of underlying library.');

        from_excel(new Path('remote://unknown_file.xlsx'));
    }
}
