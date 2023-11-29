<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_callable;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class CallbackLoaderTest extends TestCase
{
    public function test_callback_loader() : void
    {
        $rows = new Rows(
            Row::create(int_entry('number', 1), str_entry('name', 'one')),
            Row::create(int_entry('number', 2), str_entry('name', 'two')),
        );

        $data = [];

        to_callable(function (Rows $rows) use (&$data) : void {
            $data = $rows->toArray();
        })->load($rows, new FlowContext(Config::default()));

        $this->assertEquals($rows->toArray(), $data);
    }

    public function test_callback_loader_serialization() : void
    {
        $rows = new Rows(
            Row::create(int_entry('number', 1), str_entry('name', 'one')),
            Row::create(int_entry('number', 2), str_entry('name', 'two')),
        );

        $path = \sys_get_temp_dir() . DIRECTORY_SEPARATOR . \uniqid('flow_callback_loader') . '.txt';

        $loader = to_callable(function (Rows $rows) use ($path) : void {
            $data = $rows->toArray();

            \file_put_contents($path, \print_r($data, true));
        });

        $serializer = new NativePHPSerializer();
        $serializedLoader = $serializer->serialize($loader);

        $serializer->unserialize($serializedLoader)->load($rows, new FlowContext(Config::default()));

        $this->assertEquals(\print_r($rows->toArray(), true), \file_get_contents($path));

        \unlink($path);
    }
}
