<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Loader\CallbackLoader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class CallbackLoaderTest extends TestCase
{
    public function test_callback_loader() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
        );

        $data = [];

        (new CallbackLoader(function (Rows $rows) use (&$data) : void {
            $data = $rows->toArray();
        }))->load($rows);

        $this->assertEquals($rows->toArray(), $data);
    }

    public function test_callback_loader_serialization() : void
    {
        $rows = new Rows(
            Row::create(new IntegerEntry('number', 1), new StringEntry('name', 'one')),
            Row::create(new IntegerEntry('number', 2), new StringEntry('name', 'two')),
        );

        $path = \sys_get_temp_dir() . DIRECTORY_SEPARATOR . \uniqid('flow_callback_loader') . '.txt';

        $loader = new CallbackLoader(function (Rows $rows) use ($path) : void {
            $data = $rows->toArray();

            \file_put_contents($path, \print_r($data, true));
        });

        $serializer = new NativePHPSerializer();
        $serializedLoader = $serializer->serialize($loader);

        $serializer->unserialize($serializedLoader)->load($rows);

        $this->assertEquals(\print_r($rows->toArray(), true), \file_get_contents($path));

        \unlink($path);
    }
}
