<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\JSON\JsonLinesLoader;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

final class JsonLinesLoaderTest extends TestCase
{
    public function test_setting_content_type_on_path() : void
    {
        $loader = new JsonLinesLoader(path(__DIR__ . '/file.json'));

        self::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::JSON);
    }
}
