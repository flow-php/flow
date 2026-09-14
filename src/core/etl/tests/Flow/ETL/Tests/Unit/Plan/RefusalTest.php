<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Plan\Refusal;
use Flow\ETL\Tests\FlowTestCase;

final class RefusalTest extends FlowTestCase
{
    public function test_of_keeps_the_caught_instance(): void
    {
        $e = DataDependentSchemaException::step('JoinEachRowsTransformer', 'because reasons');

        static::assertSame($e, Refusal::of($e)->exception);
    }

    public function test_to_exception_returns_the_caught_instance(): void
    {
        $e = DataDependentSchemaException::step('JoinEachRowsTransformer', 'because reasons');

        static::assertSame($e, Refusal::of($e)->toException());
    }
}
