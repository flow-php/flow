<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration\Loader;

use function Flow\ETL\Adapter\XML\{from_xml, to_xml};
use function Flow\ETL\DSL\{df, overwrite};
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class XMLLoaderTest extends IntegrationTestCase
{
    public function test_xml_loader() : void
    {
        df()
            ->read(new FakeExtractor(100))
            ->saveMode(overwrite())
            ->write(to_xml($path = __DIR__ . '/var/test_xml_loader.xml'))
            ->run();

        self::assertEquals(
            100,
            df()->read(from_xml($path, 'rows/row'))->count()
        );

        //        if (\file_exists($path)) {
        //            \unlink($path);
        //        }
    }
}
