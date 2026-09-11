<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Parquet\empty_generator;
use function iterator_to_array;

final class EmptyGeneratorTest extends FlowTestCase
{
    public function test_deprecated_empty_generator_yields_nothing(): void
    {
        // @mago-expect analysis:deprecated-function
        static::assertSame([], iterator_to_array(empty_generator(), false));
    }
}
