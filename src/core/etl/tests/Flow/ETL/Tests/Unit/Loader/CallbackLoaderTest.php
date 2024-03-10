<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{int_entry, str_entry, to_callable};
use Flow\ETL\{Config, FlowContext, Row, Rows};
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

        self::assertEquals($rows->toArray(), $data);
    }
}
