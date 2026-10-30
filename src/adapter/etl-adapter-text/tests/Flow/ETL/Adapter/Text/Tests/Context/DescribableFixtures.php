<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Context;

use Closure;
use Flow\ETL\Adapter\Text\IO\Extractor\TextFileBatches;
use Flow\ETL\Adapter\Text\IO\Loader\TextLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Tests\Context\DescribableFixtures as ETLDescribableFixtures;

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
            TextLoader::class => static fn(): Describable => new TextLoader(
                path('local://data/out.txt'),
                ETLDescribableFixtures::mountedFilesystem(),
            ),
            TextFileBatches::class => static fn(): Describable => new TextFileBatches(),
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
