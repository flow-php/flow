<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{config, flow_context, row};
use function Flow\ETL\DSL\{int_entry, str_entry, to_callable};
use Flow\ETL\{Rows, Tests\FlowTestCase};

final class CallbackLoaderTest extends FlowTestCase
{
    public function test_callback_loader() : void
    {
        $rows = \Flow\ETL\DSL\rows(row(int_entry('number', 1), str_entry('name', 'one')), row(int_entry('number', 2), str_entry('name', 'two')));

        $data = [];

        to_callable(function (Rows $rows) use (&$data) : void {
            $data = $rows->toArray();
        })->load($rows, flow_context(config()));

        self::assertEquals($rows->toArray(), $data);
    }
}
