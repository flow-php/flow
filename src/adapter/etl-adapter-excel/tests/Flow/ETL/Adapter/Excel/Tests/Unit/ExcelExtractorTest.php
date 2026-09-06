<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class ExcelExtractorTest extends FlowTestCase
{
    public function test_invalid_sheet_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sheet name must be a valid Excel sheet name');

        from_excel(__DIR__ . '/../Fixtures/unknown')->withSheetName(
            'This is veeeeeery long excel sheet name, longer than 32 characters',
        );
    }

    public function test_non_local_file(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem Flow\\Filesystem\\Local\\NativeLocalFilesystem serves "file://" paths, '
            . 'given: "remote://unknown_file.xlsx".',
        );

        from_excel(path('remote://unknown_file.xlsx'));
    }

    public function test_non_local_path_on_a_matching_filesystem_still_reaches_the_open_spout_guard(): void
    {
        // the supports() guard passes because the mount matches; isLocal() is what rejects it
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Only local filesystem paths are supported by ExcelExtractor due to the limitation of underlying library.',
        );

        from_excel(path('remote://unknown_file.xlsx'), native_local_filesystem('remote'));
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('name'), str_schema('_input_file_uri')),
            from_excel(__DIR__ . '/../Fixtures/unknown')
                ->withSchema(schema(str_schema('name')))
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_source_is_the_path_it_was_given(): void
    {
        static::assertEquals(
            ExcelFixtureContext::path('fixture.xlsx'),
            from_excel(ExcelFixtureContext::file('fixture.xlsx'))->source(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('name')),
            from_excel(__DIR__ . '/../Fixtures/unknown')->withSchema(schema(str_schema('name')))->schema(),
        );
    }
}
