<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Context;

use Closure;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVFileBatches;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVReadOptions;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\IO\Inference\CSVSniffer;
use Flow\ETL\Adapter\CSV\IO\Loader\CSVLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Grid\GridHeader;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\Context\DescribableFixtures as ETLDescribableFixtures;
use Flow\Filesystem\Local\NativeLocalFilesystem;

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
            CSVFileBatches::class => static fn(): Describable => new CSVFileBatches(
                new CSVSourceOpener(
                    ETLDescribableFixtures::mountedFilesystem(),
                    new CSVReadOptions(false, false, false, ';', "'", '\\', 2048),
                ),
                ETLDescribableFixtures::inferredColumns(),
                (new SchemaInference(filesToSniff: 3, unionByName: true))->everyRow(),
            ),
            CSVLoader::class => static fn(): Describable => new CSVLoader(
                path('local://data/out.csv'),
                ETLDescribableFixtures::mountedFilesystem(),
            ),
            CSVSniffer::class => static fn(): Describable => new CSVSniffer(
                self::csvOpener(),
                [new SourceFile(path('/data/a.csv'), 400), new SourceFile(path('/data/b.csv'))],
                InferredTypes::default(),
                new GridHeader(['id', 'name'], 'a.csv'),
                new FileSplit(path('/data/a.csv'), 3, 100, 400),
            ),
            CSVSourceOpener::class => self::csvOpener(...),
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

    public static function csvOpener(): CSVSourceOpener
    {
        return new CSVSourceOpener(
            new NativeLocalFilesystem(),
            new CSVReadOptions(false, false, false, ';', "'", '\\', 2048),
        );
    }
}
