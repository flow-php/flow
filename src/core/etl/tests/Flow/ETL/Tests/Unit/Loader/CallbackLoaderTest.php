<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_callable;

final class CallbackLoaderTest extends FlowTestCase
{
    public function test_callback_loader(): void
    {
        $rows = rows(
            schema(int_schema('number'), str_schema('name')),
            row(['number' => 1, 'name' => 'one']),
            row(['number' => 2, 'name' => 'two']),
        );

        $data = [];

        to_callable(function (Rows $rows) use (&$data): void {
            $data = $rows->toArray();
        })->load($rows, flow_context(config()));

        static::assertEquals($rows->toArray(), $data);
    }
}
