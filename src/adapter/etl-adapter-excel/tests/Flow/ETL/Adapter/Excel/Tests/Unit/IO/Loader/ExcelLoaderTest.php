<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\IO\Loader;

use Flow\ETL\Adapter\Excel\IO\Loader\ExcelLoader;
use Flow\ETL\Adapter\Excel\IO\Loader\ExcelWriter;
use Flow\ETL\Adapter\Excel\Tests\Double\RecordingCellStyler;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\XLSX\Options;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;

final class ExcelLoaderTest extends FlowTestCase
{
    public static function loaders_holding_an_object_that_does_not_describe(): Generator
    {
        yield 'cell styler' => [to_excel('/data/out.xlsx')->withCellStyler(new RecordingCellStyler())];
        yield 'header style' => [to_excel('/data/out.xlsx')->withHeaderStyle(new Style())];
        yield 'writer options' => [to_excel('/data/out.xlsx')->withWriterOptions(new Options())];
    }

    public static function loaders_with_every_plain_setting(): Generator
    {
        yield 'sheet name' => [
            to_excel('/data/out.xlsx')
                ->saveMode(overwrite())
                ->partitionBy(partition_by('country'))
                ->withDateFormat('dd/mm/yyyy')
                ->withDateTimeFormat('dd/mm/yyyy hh:mm')
                ->withHeader(false)
                ->withTimeFormat('%H:%I')
                ->withSheetName('Orders')
                ->withWriter(ExcelWriter::ODS)
                ->inTask(new Part(1, 2, 'run1', false)),
        ];
        yield 'sheet name column' => [
            to_excel('/data/out.xlsx')->withSheetNameFromColumn('sheet')->inTask(new Part(0, 1, 'run2', true)),
        ];
    }

    #[DataProvider('loaders_holding_an_object_that_does_not_describe')]
    public function test_a_loader_holding_an_object_that_does_not_describe_does_not_describe(ExcelLoader $loader): void
    {
        static::assertNull($loader->describe());
    }

    #[DataProvider('loaders_with_every_plain_setting')]
    public function test_every_plain_setting_survives_describe(ExcelLoader $loader): void
    {
        static::assertEquals($loader->describe(), DescribeRoundTrip::rebuilt($loader)->describe());
    }
}
