<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Flow\ETL\Adapter\Excel\ExcelFormatDetector;
use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Filesystem\DSL\native_local_filesystem;

final class ExcelFormatDetectorTest extends FlowTestCase
{
    public function test_a_zip_signed_file_that_cannot_be_opened_is_an_unsupported_format(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported file format: n/a');

        (new ExcelFormatDetector(native_local_filesystem()))->detect(ExcelFixtureContext::path('corrupt_zip'));
    }

    public function test_closes_the_stream_it_sniffed(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        (new ExcelFormatDetector($filesystem))->detect(ExcelFixtureContext::path('sniff/a'));

        static::assertSame(1, $filesystem->readFromCalls);
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_detects_ods_from_the_zip_mimetype(): void
    {
        static::assertSame(
            ExcelReader::ODS,
            (new ExcelFormatDetector(native_local_filesystem()))->detect(ExcelFixtureContext::path('fixture_as_ods')),
        );
    }

    public function test_detects_the_legacy_xls_signature_as_xlsx(): void
    {
        static::assertSame(
            ExcelReader::XLSX,
            (new ExcelFormatDetector(native_local_filesystem()))->detect(ExcelFixtureContext::path('xls_signature')),
        );
    }

    public function test_detects_xlsx_from_the_zip_signature(): void
    {
        static::assertSame(
            ExcelReader::XLSX,
            (new ExcelFormatDetector(native_local_filesystem()))->detect(ExcelFixtureContext::path('fixture_as_xlsx')),
        );
    }

    public function test_refuses_an_unknown_format(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported file format: n/a');

        (new ExcelFormatDetector(native_local_filesystem()))->detect(ExcelFixtureContext::path('empty_file'));
    }
}
