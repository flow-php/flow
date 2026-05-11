<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use Flow\ETL\Adapter\XML\Loader\XMLLoader;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;

final class XMLLoaderTest extends TestCase
{
    public function test_setting_content_type_on_path(): void
    {
        $loader = new XMLLoader(path(__DIR__ . '/file.csv'), new DOMDocumentWriter());

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::XML);
    }
}
