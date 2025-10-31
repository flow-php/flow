<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\XML\Loader\XMLLoader;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

final class XMLLoaderTest extends TestCase
{
    public function test_setting_content_type_on_path() : void
    {
        $loader = new XMLLoader(path(__DIR__ . '/file.csv'), new DOMDocumentWriter());

        self::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::XML);
    }
}
