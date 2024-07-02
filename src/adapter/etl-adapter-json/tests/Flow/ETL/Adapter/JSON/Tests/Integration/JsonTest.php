<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\Json\to_json;
use function Flow\ETL\DSL\df;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Config, FlowContext, Rows};
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class JsonTest extends TestCase
{
    public function test_json_loader() : void
    {
        $path = \sys_get_temp_dir() . '/flow_php_etl_json_loader' . bin2hex(random_bytes(16)) . '.json';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json($path))
            ->run();

        self::assertEquals(
            100,
            df()->read(from_json($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_json_loader_loading_empty_string() : void
    {
        $stream = \sys_get_temp_dir() . '/flow_php_etl_json_loader' . bin2hex(random_bytes(16)) . '.json';

        $loader = new JsonLoader(Path::realpath($stream));

        $loader->load(new Rows(), $context = new FlowContext(Config::default()));

        $loader->closure($context);

        self::assertJsonStringEqualsJsonString(
            <<<'JSON'
[
]
JSON,
            \file_get_contents($stream)
        );

        if (\file_exists($stream)) {
            \unlink($stream);
        }
    }
}
