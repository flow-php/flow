<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use Flow\ETL\Adapter\Excel\ExcelFormatDetector;
use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class ExcelFormatDetectorTest extends FlowTestCase
{
    #[TestWith(['orders.xlsx', ExcelReader::XLSX])]
    #[TestWith(['orders.ods', ExcelReader::ODS])]
    public function test_detects_by_extension(string $file, ExcelReader $expected): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        static::assertSame($expected, (new ExcelFormatDetector($filesystem))->detect(path('/does/not/exist/' . $file)));
        static::assertSame(0, $filesystem->readFromCalls);
    }
}
