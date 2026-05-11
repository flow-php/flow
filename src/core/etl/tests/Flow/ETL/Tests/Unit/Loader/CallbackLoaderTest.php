<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_callable;

final class CallbackLoaderTest extends FlowTestCase
{
    public function test_callback_loader(): void
    {
        $rows = \Flow\ETL\DSL\rows(
            row(int_entry('number', 1), str_entry('name', 'one')),
            row(int_entry('number', 2), str_entry('name', 'two')),
        );

        $data = [];

        to_callable(function (Rows $rows) use (&$data): void {
            $data = $rows->toArray();
        })->load($rows, flow_context(config()));

        static::assertEquals($rows->toArray(), $data);
    }
}
