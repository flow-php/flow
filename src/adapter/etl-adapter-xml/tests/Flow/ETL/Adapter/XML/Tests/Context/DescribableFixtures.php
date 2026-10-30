<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Context;

use Closure;
use Flow\ETL\Adapter\XML\IO\Extractor\XMLFileBatches;
use Flow\ETL\Adapter\XML\IO\Loader\XMLLoader;
use Flow\ETL\Adapter\XML\IO\Loader\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Adapter\XML\IO\Loader\XMLWriter\StringXMLWriter;
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
            DOMDocumentWriter::class => static fn(): Describable => new DOMDocumentWriter(),
            StringXMLWriter::class => static fn(): Describable => new StringXMLWriter(),
            XMLLoader::class => static fn(): Describable => new XMLLoader(
                path('local://data/out.xml'),
                new StringXMLWriter(),
                ETLDescribableFixtures::mountedFilesystem(),
            ),
            XMLFileBatches::class => static fn(): Describable => new XMLFileBatches('root/item', 4096),
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
