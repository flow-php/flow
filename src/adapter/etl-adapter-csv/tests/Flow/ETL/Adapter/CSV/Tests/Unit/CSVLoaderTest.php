<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

final class CSVLoaderTest extends TestCase
{
    public function test_setting_content_type_on_path() : void
    {
        $loader = new CSVLoader(path(__DIR__ . '/file.csv'));

        self::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::CSV);
    }
}
