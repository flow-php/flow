<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Output;

use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Bridge\Symfony\HttpFoundation\http_csv_output;

final class CSVOutputTest extends FlowTestCase
{
    public function test_type(): void
    {
        static::assertSame(Type::CSV, http_csv_output()->type());
    }
}
