<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Adapter\JSON;

use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Config;
use Flow\ETL\DSL\Json;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class JsonLoaderTest extends TestCase
{
    public function test_json_loader() : void
    {
        $stream = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader', true) . '.json';

        (new Flow())
            ->process(
                new Rows(
                    ...\array_map(
                        fn (int $i) : Row => Row::create(
                            new Row\Entry\IntegerEntry('id', $i),
                            new Row\Entry\StringEntry('name', 'name_' . $i)
                        ),
                        \range(0, 10)
                    )
                )
            )
            ->write(Json::to($stream))
            ->run();

        $this->assertJsonStringEqualsJsonString(
            <<<'JSON'
[
  {"id":0,"name":"name_0"},
  {"id":1,"name":"name_1"},
  {"id":2,"name":"name_2"},
  {"id":3,"name":"name_3"},
  {"id":4,"name":"name_4"},
  {"id":5,"name":"name_5"},
  {"id":6,"name":"name_6"},
  {"id":7,"name":"name_7"},
  {"id":8,"name":"name_8"},
  {"id":9,"name":"name_9"},
  {"id":10,"name":"name_10"}
]
JSON,
            \file_get_contents($stream)
        );

        if (\file_exists($stream)) {
            \unlink($stream);
        }
    }

    public function test_json_loader_loading_empty_string() : void
    {
        $stream = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader', true) . '.json';

        $loader = new JsonLoader(Path::realpath($stream));

        $loader->load(new Rows(), $context = new FlowContext(Config::default()));

        $loader->closure(new Rows(), $context);

        $this->assertJsonStringEqualsJsonString(
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

    public function test_json_loader_with_a_thread_safe_and_append_mode() : void
    {
        $stream = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader', true) . '.json';

        \file_put_contents($stream, '[]');

        $loader = new JsonLoader(Path::realpath($stream));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Appending to existing single file destination \"file:/{$stream}\" in non thread safe mode is not supported.");

        (new Flow())
            ->process(
                new Rows(
                    ...\array_map(
                        fn (int $i) : Row => Row::create(
                            new Row\Entry\IntegerEntry('id', $i),
                            new Row\Entry\StringEntry('name', 'name_' . $i)
                        ),
                        \range(0, 5)
                    )
                )
            )
            ->mode(SaveMode::Append)
            ->threadSafe()
            ->load($loader)
            ->run();

        if (\file_exists($stream)) {
            \unlink($stream);
        }
    }

    public function test_json_loader_with_a_thread_safe_and_overwrite() : void
    {
        $stream = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader', true) . '.json';

        $loader = new JsonLoader(Path::realpath($stream));

        $loader->load(
            new Rows(
                ...\array_map(
                    fn (int $i) : Row => Row::create(
                        new Row\Entry\IntegerEntry('id', $i),
                        new Row\Entry\StringEntry('name', 'name_' . $i)
                    ),
                    \range(0, 5)
                )
            ),
            ($context = new FlowContext(Config::default()))->setThreadSafe()
        );

        $loader->load(
            new Rows(
                ...\array_map(
                    fn (int $i) : Row => Row::create(
                        new Row\Entry\IntegerEntry('id', $i),
                        new Row\Entry\StringEntry('name', 'name_' . $i)
                    ),
                    \range(6, 10)
                )
            ),
            $context = ($context)->setMode(SaveMode::Overwrite)->setThreadSafe()
        );

        $loader->closure(new Rows(), $context);

        $files = \array_values(\array_diff(\scandir($stream), ['..', '.']));

        $this->assertJsonStringEqualsJsonString(
            <<<'JSON'
[
      {"id":0,"name":"name_0"},
      {"id":1,"name":"name_1"},
      {"id":2,"name":"name_2"},
      {"id":3,"name":"name_3"},
      {"id":4,"name":"name_4"},
      {"id":5,"name":"name_5"},
      {"id":6,"name":"name_6"},
      {"id":7,"name":"name_7"},
      {"id":8,"name":"name_8"},
      {"id":9,"name":"name_9"},
      {"id":10,"name":"name_10"}
]
JSON,
            \file_get_contents($stream . DIRECTORY_SEPARATOR . $files[0])
        );

        if (\file_exists($stream . DIRECTORY_SEPARATOR . $files[0])) {
            \unlink($stream . DIRECTORY_SEPARATOR . $files[0]);
        }
    }
}
