<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\Text\TextLoader;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

final class TextLoaderTest extends TestCase
{
    public function test_setting_content_type_on_path() : void
    {
        $loader = new TextLoader(path(__DIR__ . '/file.txt'));

        self::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::TEXT);
    }
}
