<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Output;

use function Flow\Bridge\Symfony\HttpFoundation\http_json_output;
use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Tests\FlowTestCase;

final class JsonOutputTest extends FlowTestCase
{
    public function test_type() : void
    {
        self::assertSame(Type::JSON, http_json_output()->type());
    }
}
