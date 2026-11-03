<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Runtime\Metrics\Report;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\run;

final class RunTest extends FlowTestCase
{
    public function test_run_returns_no_report_without_analyze(): void
    {
        static::assertNull(run()(df()->read(from_array([['a' => 1], ['a' => 2]]))));
    }

    public function test_run_returns_a_report_with_analyze(): void
    {
        static::assertInstanceOf(Report::class, run(analyze())(df()->read(from_array([['a' => 1], ['a' => 2]]))));
    }
}
