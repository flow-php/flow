<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use function Flow\ETL\Adapter\Json\from_json;
use function Flow\ETL\Adapter\Json\to_json;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Config;
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
            ->write(to_json($stream))
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

        $loader->closure($context);

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
            $context = $context->setAppendSafe()
        );

        $loader->closure($context);

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

    public function test_save_mode_ignore_on_partitioned_rows() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader_ignore_mode', true);

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(from_array([
                ['id' => 1, 'partition' => 'a'],
                ['id' => 2, 'partition' => 'a'],
                ['id' => 3, 'partition' => 'a'],
                ['id' => 4, 'partition' => 'b'],
                ['id' => 5, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(SaveMode::Overwrite)
            ->write(to_json($path))
            ->run();

        df()
            ->read(from_array([
                ['id' => 8, 'partition' => 'b'],
                ['id' => 10, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(SaveMode::Ignore)
            ->write(to_json($path))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'partition' => 'a'],
                ['id' => 2, 'partition' => 'a'],
                ['id' => 3, 'partition' => 'a'],
                ['id' => 4, 'partition' => 'b'],
                ['id' => 5, 'partition' => 'b'],
            ],
            df()
                ->read(from_json($path))
                ->fetch()
                ->toArray()
        );
    }

    public function test_save_mode_overwrite_on_partitioned_rows() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader_ignore_mode', true);

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow)
            ->read(from_array([
                ['id' => 1, 'partition' => 'a'],
                ['id' => 2, 'partition' => 'a'],
                ['id' => 3, 'partition' => 'a'],
                ['id' => 4, 'partition' => 'b'],
                ['id' => 5, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(SaveMode::Overwrite)
            ->write(to_json($path))
            ->run();

        (new Flow)
            ->read(from_array([
                ['id' => 8, 'partition' => 'b'],
                ['id' => 10, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(SaveMode::Overwrite)
            ->write(to_json($path))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'partition' => 'a'],
                ['id' => 2, 'partition' => 'a'],
                ['id' => 3, 'partition' => 'a'],
                ['id' => 8, 'partition' => 'b'],
                ['id' => 10, 'partition' => 'b'],
            ],
            (new Flow())
                ->read(from_json($path))
                ->fetch()
                ->toArray()
        );
    }

    public function test_save_mode_throw_exception_on_partitioned_rows() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader_exception_mode', true);

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow)
            ->read(from_array([
                ['id' => 1, 'partition' => 'a'],
                ['id' => 2, 'partition' => 'a'],
                ['id' => 3, 'partition' => 'a'],
                ['id' => 4, 'partition' => 'b'],
                ['id' => 5, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(SaveMode::ExceptionIfExists)
            ->write(to_json($path))
            ->run();

        $this->expectExceptionMessage('Destination path "file:/' . $path . '/partition=b" already exists, please change path to different or set different SaveMode');

        (new Flow)
            ->read(from_array([
                ['id' => 8, 'partition' => 'b'],
                ['id' => 10, 'partition' => 'b'],
            ]))
            ->partitionBy(ref('partition'))
            ->mode(SaveMode::ExceptionIfExists)
            ->write(to_json($path))
            ->run();

    }

    public function test_save_with_ignore_mode() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_json_loader_ignore_mode', true) . '.json';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow)
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->mode(SaveMode::Ignore)
            ->write(to_json($path))
            ->run();

        (new Flow)
            ->read(from_array([
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ]))
            ->mode(SaveMode::Ignore)
            ->write(to_json($path))
            ->run();

        $this->assertSame(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            (new Flow)
                ->read(from_json($path))
                ->fetch()
                ->toArray()
        );
    }
}
