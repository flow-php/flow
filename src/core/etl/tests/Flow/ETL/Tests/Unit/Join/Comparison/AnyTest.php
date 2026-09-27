<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class AnyTest extends FlowTestCase
{
    public function test_failure(): void
    {
        $comparison1 = self::createStub(Comparison::class);
        $comparison1->method('compare')->willReturn(false);

        $comparison2 = self::createStub(Comparison::class);
        $comparison2->method('compare')->willReturn(false);

        static::assertFalse((new Any($comparison1, $comparison2))->compare(
            array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => 2], schema(int_schema('id'))),
        ));
    }

    public function test_success(): void
    {
        $comparison1 = self::createStub(Comparison::class);
        $comparison1->method('compare')->willReturn(true);

        $comparison2 = self::createStub(Comparison::class);
        $comparison2->method('compare')->willReturn(false);

        static::assertTrue((new Any($comparison1, $comparison2))->compare(
            array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => 2], schema(int_schema('id'))),
        ));
    }
}
