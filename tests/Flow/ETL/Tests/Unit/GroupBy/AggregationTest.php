<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregation;
use PHPUnit\Framework\TestCase;

final class AggregationTest extends TestCase
{
    public function test_creating_invalid_aggregation() : void
    {
        $this->expectExceptionMessage("Unknown aggregation \"test\", expected one of: 'avg', 'count', 'max', 'min', 'sum'");
        $this->expectException(InvalidArgumentException::class);

        new Aggregation('test', 'entry');
    }
}
