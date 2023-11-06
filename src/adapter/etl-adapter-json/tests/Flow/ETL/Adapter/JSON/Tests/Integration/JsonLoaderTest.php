<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

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
use Flow\ETL\Test\FilesystemTestHelper;
use PHPUnit\Framework\TestCase;

final class JsonLoaderTest extends TestCase
{
    use FilesystemTestHelper;

    public function test_json_loader() : void
    {
        $path = $this->createTemporaryFile('flow_php_etl_json_loader', '.json');

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
            ->write(Json::to($path))
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
            \file_get_contents($path)
        );

        $this->removeFile($path);
    }

    public function test_json_loader_loading_empty_string() : void
    {
        $path = $this->createTemporaryFile('flow_php_etl_json_loader', '.json');

        $loader = new JsonLoader(Path::realpath($path));

        $loader->load(new Rows(), $context = new FlowContext(Config::default()));

        $loader->closure($context);

        $this->assertJsonStringEqualsJsonString(
            <<<'JSON'
[
]
JSON,
            \file_get_contents($path)
        );

        $this->removeFile($path);
    }

    public function test_json_loader_with_a_thread_safe_and_overwrite() : void
    {
        $path = $this->createTemporaryFile('flow_php_etl_json_loader', '.json');

        $loader = new JsonLoader(Path::realpath($path));

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
            ($context = new FlowContext(Config::default()))->setAppendSafe()
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
            $context = ($context)->setMode(SaveMode::Overwrite)->setAppendSafe()
        );

        $loader->closure($context);

        $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

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
            \file_get_contents($path . DIRECTORY_SEPARATOR . $files[0])
        );

        $this->removeFile($path . DIRECTORY_SEPARATOR . $files[0]);
    }

    public function test_json_loader_with_append_mode() : void
    {
        $path = $this->createTemporaryFile('flow_php_etl_json_loader', '.json');

        \file_put_contents($path, '[]');

        $loader = new JsonLoader(Path::realpath($path));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Appending to existing single file destination \"file:/{$path}\" is not supported.");

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
            ->load($loader)
            ->run();

        $this->removeFile($path);
    }
}
