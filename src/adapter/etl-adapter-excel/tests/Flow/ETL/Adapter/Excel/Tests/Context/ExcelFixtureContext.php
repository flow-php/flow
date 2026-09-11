<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Context;

use Flow\ETL\Adapter\Excel\CellTypeNarrower;
use Flow\ETL\Adapter\Excel\ExcelFormatDetector;
use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Adapter\Excel\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\Sheet\SheetCells;
use Flow\ETL\Adapter\Excel\Sheet\SheetsManager;
use Flow\ETL\Adapter\Excel\WorkbookReader;
use Flow\ETL\Adapter\Excel\WorkbookSampler;
use Flow\ETL\Adapter\Excel\WorkbookSheet;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
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
use function sys_get_temp_dir;

final class ExcelFixtureContext
{
    public static function infer(WorkbookSampler $sampler, SchemaInference $inference = new SchemaInference()): Schema
    {
        return (new SchemaInferrer($inference, new CellTypeNarrower($inference->candidates())))->infer(
            $sampler->header()->names,
            $sampler->samples($inference->sampleSize),
        );
    }

    public static function globSampler(
        string $glob,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ExcelReadOptions $options = new ExcelReadOptions(),
    ): WorkbookSampler {
        return new WorkbookSampler(self::reader($filesystem, $options), self::globSources($glob, $filesystem));
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
            $sources[] = new SourceFile($status->path);
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
     * Folders that appeared since $before. Only additions count - the temp dir is machine-global, so another
     * process may reclaim an unrelated one while a test runs.
     *
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

    public static function sampler(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ExcelReadOptions $options = new ExcelReadOptions(),
    ): WorkbookSampler {
        return new WorkbookSampler(self::reader($filesystem, $options), [self::source($fixture)]);
    }

    public static function openSheetCells(string $fixture, bool $withHeader, int $offset): OpenSheetCells
    {
        $reader = (new ExcelFormatDetector(new NativeLocalFilesystem()))->detect(self::path($fixture))
        === ExcelReader::ODS
            ? new OdsReader()
            : new XlsxReader();

        $reader->open(self::path($fixture)->path());

        return new OpenSheetCells(
            $reader,
            new SheetCells((new SheetsManager($reader->getSheetIterator()))->first(), $withHeader, $offset),
        );
    }

    public static function sheet(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ExcelReadOptions $options = new ExcelReadOptions(),
    ): WorkbookSheet {
        return self::reader($filesystem, $options)->sheet(self::source($fixture));
    }

    public static function source(string $fixture): SourceFile
    {
        return new SourceFile(self::path($fixture));
    }
}
