<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;

final class CSVLoaderTest extends TestCase
{
    public function test_setting_content_type_on_path(): void
    {
        $loader = new CSVLoader(path(__DIR__ . '/file.csv'));

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::CSV);
    }
}
