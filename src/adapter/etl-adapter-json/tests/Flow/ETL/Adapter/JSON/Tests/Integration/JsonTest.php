<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\Json\to_json;
use function Flow\ETL\DSL\{df, overwrite};
use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Config, FlowContext, Rows};
use PHPUnit\Framework\TestCase;

final class JsonTest extends TestCase
{
    public function test_json_loader() : void
    {

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json($path = __DIR__ . '/var/test_json_loader.json'))
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
        $loader = new JsonLoader(path($path = __DIR__ . '/var/test_json_loader_loading_empty_string.json'));

        $loader->load(new Rows(), $context = new FlowContext(Config::default()));

        $loader->closure($context);

        self::assertJsonStringEqualsJsonString(
            <<<'JSON'
[
]
JSON,
            \file_get_contents($path)
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_json_loader_overwrite_mode() : void
    {

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json($path = __DIR__ . '/var/test_json_loader.json'))
            ->run();

        df()
            ->read(new FakeExtractor(100))
            ->mode(overwrite())
            ->write(to_json($path = __DIR__ . '/var/test_json_loader.json'))
            ->run();

        $content = \file_get_contents($path);
        self::stringEndsWith(']', $content);

        self::assertEquals(
            100,
            df()->read(from_json($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }
}
