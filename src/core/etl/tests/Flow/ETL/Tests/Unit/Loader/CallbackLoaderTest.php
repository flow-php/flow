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
}
