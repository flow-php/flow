<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Context;

use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelFormatDetector;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReader;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\SheetCells;
use Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\SheetsManager;
use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookReader;
use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookSheet;
use Flow\ETL\Adapter\Excel\IO\Inference\ExcelSniffer;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;

use function array_diff;
use function array_values;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function glob;
use function is_array;
use function sys_get_temp_dir;

final class ExcelFixtureContext
{
    public static function infer(ExcelSniffer $sniffer, SchemaInference $inference = new SchemaInference()): Schema
    {
        return (new SchemaInferrer($inference))->infer($sniffer)->schema;
    }

    /**
     * @param SchemaInference $inference its candidates build the narrower, as ExcelExtractor builds it
     */
    public static function globSniffer(
        string $glob,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ExcelReadOptions $options = new ExcelReadOptions(),
        SchemaInference $inference = new SchemaInference(),
    ): ExcelSniffer {
        return new ExcelSniffer(
            self::reader($filesystem, $options),
            self::globSources($glob, $filesystem),
            $inference->candidates(),
        );
    }

    /**
     * The materialised listing both read paths take.
     *
     * @return list<SourceFile>
     */
    public static function globSources(string $glob, Filesystem $filesystem = new NativeLocalFilesystem()): array
    {
        $sources = [];

        foreach ((new FileListing($filesystem))->list(path(self::file($glob)), new OnlyFiles()) as $status) {
            $sources[] = new SourceFile($status->path, $status->size);
        }

        return $sources;
    }

    public static function file(string $fixture): string
    {
        return __DIR__ . '/../Fixtures/' . $fixture;
    }

    public static function path(string $fixture): Path
    {
        return path_real(self::file($fixture));
    }

    /**
     * OpenSpout's file-based shared-strings cache writes one folder per open reader and removes it on close,
     * so a leaked folder is a reader that was never closed.
     *
     * @return list<string>
     */
    public static function sharedStringsFolders(): array
    {
        return glob(sys_get_temp_dir() . '/sharedstrings*', GLOB_ONLYDIR) ?: [];
    }

    /**
     * @param list<string> $before
     *
     * @return list<string>
     */
    public static function leakedSharedStringsFoldersSince(array $before): array
    {
        return array_values(array_diff(self::sharedStringsFolders(), $before));
    }

    public static function reader(
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ExcelReadOptions $options = new ExcelReadOptions(),
    ): WorkbookReader {
        return new WorkbookReader($options, new ExcelFormatDetector($filesystem));
    }

    /**
     * @param SchemaInference $inference its candidates build the narrower, as ExcelExtractor builds it
     */
    public static function sniffer(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ExcelReadOptions $options = new ExcelReadOptions(),
        SchemaInference $inference = new SchemaInference(),
    ): ExcelSniffer {
        return new ExcelSniffer(
            self::reader($filesystem, $options),
            [self::source($fixture)],
            $inference->candidates(),
        );
    }

    public static function openSheetCells(string $fixture, bool $withHeader): OpenSheetCells
    {
        $reader = (new ExcelFormatDetector(new NativeLocalFilesystem()))->detect(self::path($fixture))
        === ExcelReader::ODS
            ? new OdsReader()
            : new XlsxReader();

        $reader->open(self::path($fixture)->path());

        return new OpenSheetCells(
            $reader,
            new SheetCells((new SheetsManager($reader->getSheetIterator()))->first(), $withHeader),
        );
    }

    public static function sheet(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ExcelReadOptions $options = new ExcelReadOptions(),
    ): WorkbookSheet {
        return self::reader($filesystem, $options)->sheet(self::source($fixture));
    }

    /**
     * The rows of $sheet's records that are not corrupted, decoded as the read decodes them.
     *
     * @return list<array<string, mixed>>
     */
    public static function decodedRows(WorkbookSheet $sheet): array
    {
        $rows = [];

        foreach ($sheet->rows() as $cells) {
            $values = $sheet->decode($cells);

            if (is_array($values)) {
                $rows[] = $values;
            }
        }

        return $rows;
    }

    public static function source(string $fixture): SourceFile
    {
        return new SourceFile(self::path($fixture));
    }
}
