<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\ExplainedRows;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\SpyClient;
use Flow\ETL\Cardinality;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\AST\Transformers\ExplainConfig;

final class ExplainedRowsTest extends FlowTestCase
{
    public function test_a_maximum_above_the_estimate_only_bounds_it(): void
    {
        static::assertEquals(
            new Cardinality(atMost: 50, estimate: 7, relativeError: Cardinality::DEFAULT_RELATIVE_ERROR),
            (new ExplainedRows())->of((new SpyClient())->willExplain(7), 'SELECT 1', maximum: 50),
        );
    }

    public function test_a_maximum_below_the_estimate_caps_it(): void
    {
        static::assertEquals(
            new Cardinality(atMost: 5, estimate: 5, relativeError: Cardinality::DEFAULT_RELATIVE_ERROR),
            (new ExplainedRows())->of((new SpyClient())->willExplain(7), 'SELECT 1', maximum: 5),
        );
    }

    public function test_the_plan_is_asked_for_an_estimate_only(): void
    {
        $client = (new SpyClient())->willExplain(7);

        static::assertEquals(Cardinality::approximately(7), (new ExplainedRows())->of($client, 'SELECT $1', [3]));
        static::assertEquals(
            [['sql' => 'SELECT $1', 'parameters' => [3], 'config' => ExplainConfig::forEstimate()]],
            $client->explained,
        );
    }
}
