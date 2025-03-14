<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Output;

use function Flow\Bridge\Symfony\HttpFoundation\http_parquet_output;
use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Tests\FlowTestCase;

final class ParquetOutputTest extends FlowTestCase
{
    public function test_type() : void
    {
        self::assertSame(Type::PARQUET, http_parquet_output()->type());
    }
}
