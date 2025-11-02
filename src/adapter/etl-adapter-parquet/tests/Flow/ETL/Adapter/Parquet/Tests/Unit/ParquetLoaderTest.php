<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\Parquet\ParquetLoader;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

final class ParquetLoaderTest extends TestCase
{
    public function test_setting_content_type_on_path() : void
    {
        $loader = new ParquetLoader(path(__DIR__ . '/file.parquet'));

        self::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::PARQUET);
    }
}
