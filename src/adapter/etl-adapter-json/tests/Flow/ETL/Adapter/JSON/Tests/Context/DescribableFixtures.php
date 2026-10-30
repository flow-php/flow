<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Context;

use Closure;
use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileBatches;
use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileReader;
use Flow\ETL\Adapter\JSON\IO\Inference\JsonSniffer;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\IO\Loader\JsonLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
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
            JsonFileBatches::class => static fn(): Describable => new JsonFileBatches(
                ETLDescribableFixtures::mountedFilesystem(),
                self::jsonReader(),
            ),
            JsonFileReader::class => self::jsonReader(...),
            JsonLoader::class => static fn(): Describable => new JsonLoader(
                path('local://data/out.json'),
                ETLDescribableFixtures::mountedFilesystem(),
                JsonFraming::Lines,
            ),
            JsonSniffer::class => static fn(): Describable => new JsonSniffer(
                self::jsonReader(),
                [new SourceFile(path('/data/a.json'), 400)],
                new FileSplit(path('/data/a.json'), 3, 100, 400),
            ),
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

    public static function jsonReader(): JsonFileReader
    {
        return new JsonFileReader(new NativeLocalFilesystem(), JsonFraming::Lines, '/items', true);
    }
}
