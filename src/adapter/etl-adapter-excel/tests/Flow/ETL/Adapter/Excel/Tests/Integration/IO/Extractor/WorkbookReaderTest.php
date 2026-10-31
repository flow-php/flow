<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelFormatDetector;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookReader;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;

use function array_keys;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Types\DSL\type_array;

final class WorkbookReaderTest extends FlowTestCase
{
    public function test_options_reach_the_sheet(): void
    {
        $rows = ExcelFixtureContext::decodedRows(ExcelFixtureContext::sheet(
            'nullable_fixture.xlsx',
            options: new ExcelReadOptions(withHeader: false, convertEmptyToNull: false),
        ));

        static::assertCount(6, $rows);
        static::assertSame(['e00', 'e01', 'e02'], array_keys($rows[0]));
        static::assertSame('', $rows[3]['e01']);
    }

    public function test_sheet_is_a_new_object_every_time(): void
    {
        $reader = ExcelFixtureContext::reader();
        $source = ExcelFixtureContext::source('fixture.xlsx');

        static::assertNotSame($reader->sheet($source), $reader->sheet($source));
    }

    public function test_a_workbook_reader_over_the_local_filesystem_describes_and_rebuilds_equal(): void
    {
        $reader = new WorkbookReader(
            (new ExcelReadOptions())->withHeader(false),
            new ExcelFormatDetector(new NativeLocalFilesystem()),
        );
        $description = $reader->describe();

        static::assertNotNull($description);
        static::assertFalse(type_array()->assert($description->data['options'])['with_header']);
        static::assertEquals($reader, (new Rebuild())->object(
            Description::fromArray($description->normalize()),
            WorkbookReader::class,
        ));
    }

    public function test_a_workbook_reader_over_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(
            (new WorkbookReader(new ExcelReadOptions(), new ExcelFormatDetector(memory_filesystem())))->describe(),
        );
    }

    public function test_a_workbook_reader_declares_the_extensions_a_workbook_is_read_with(): void
    {
        static::assertSame(
            ['dom', 'filter', 'xmlreader', 'zip'],
            (new WorkbookReader(new ExcelReadOptions(), new ExcelFormatDetector(new NativeLocalFilesystem())))
                ->describe()
                ?->extensions(),
        );
    }
}
