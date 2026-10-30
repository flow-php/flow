<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Context;

use Closure;
use Flow\ETL\Adapter\Excel\Functions\IsValidExcelSheetName;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelFileBatches;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelFormatDetector;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReader;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookReader;
use Flow\ETL\Adapter\Excel\IO\Inference\ExcelSniffer;
use Flow\ETL\Adapter\Excel\IO\Loader\ExcelLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Grid\GridHeader;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\Tests\Context\DescribableFixtures as ETLDescribableFixtures;
use Flow\Filesystem\Local\NativeLocalFilesystem;

use function Flow\ETL\DSL\ref;
use function Flow\Filesystem\DSL\path;

/**
 * One instance of every Describable this package ships, with every constructor argument away from its default, so a
 * parameter that describe() or fromDescription() forgets changes the rebuilt object.
 */
final class DescribableFixtures
{
    /**
     * @return array<class-string<Describable>, Closure(): Describable>
     */
    public static function all(): array
    {
        return [
            IsValidExcelSheetName::class => static fn(): Describable => new IsValidExcelSheetName(ref('string')),
            ExcelLoader::class => static fn(): Describable => new ExcelLoader(path('/data/out.xlsx')),
            ExcelSniffer::class => static fn(): Describable => new ExcelSniffer(
                self::workbook(),
                [new SourceFile(path('/data/a.xlsx'), 400)],
                InferredTypes::default(),
                new GridHeader(['id', 'name'], 'a.xlsx'),
            ),
            ExcelFileBatches::class => static fn(): Describable => new ExcelFileBatches(
                self::workbook(),
                null,
                ETLDescribableFixtures::inferredColumns(),
            ),
            WorkbookReader::class => self::workbook(...),
        ];
    }

    /**
     * @return list<class-string<Describable>>
     */
    public static function undescribable(): array
    {
        return [];
    }

    /**
     * @return list<class-string<Describable>>
     */
    public static function unconstructable(): array
    {
        return [];
    }

    public static function excelOptions(): ExcelReadOptions
    {
        return new ExcelReadOptions(false, false, 'Orders', ExcelReader::ODS);
    }

    public static function workbook(): WorkbookReader
    {
        return new WorkbookReader(self::excelOptions(), new ExcelFormatDetector(new NativeLocalFilesystem()));
    }
}
