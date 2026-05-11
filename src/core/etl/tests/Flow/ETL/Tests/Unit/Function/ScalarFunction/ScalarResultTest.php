<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\ScalarFunction;

use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;

final class ScalarResultTest extends FlowTestCase
{
    public function test_scalar_result_for_null_value_and_not_null_type(): void
    {
        $result = new ScalarResult(null, type_float());

        static::assertEquals($result->type, type_optional(type_float()));
    }
}
