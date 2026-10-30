<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Closure;
use Flow\ETL\Adapter\Parquet\IO\ConfiguredParquetEngine;
use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetFileBatches;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\Tests\Context\DescribableFixtures as ETLDescribableFixtures;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options as ParquetOptions;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
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
            ConfiguredParquetEngine::class => static fn(): Describable => new ConfiguredParquetEngine(
                new PhpParquetEngine(),
            ),
            ParquetFileBatches::class => static fn(): Describable => new ParquetFileBatches(
                new PhpParquetEngine(),
                ParquetOptions::default(),
                ['id', 'country'],
                new DerivedSchema(schema(int_schema('id')), 'a.parquet'),
            ),
            ParquetLoader::class => static fn(): Describable => new ParquetLoader(
                path('local://data/out.parquet'),
                ETLDescribableFixtures::mountedFilesystem(),
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
}
