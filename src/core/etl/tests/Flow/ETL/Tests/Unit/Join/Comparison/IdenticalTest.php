<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\row;

final class IdenticalTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertFalse((new Identical('id', 'id'))->compare(row(['id' => 1]), row(['id' => 2])));
    }

    public function test_null_is_not_identical_to_null(): void
    {
        static::assertFalse((new Identical('id', 'id'))->compare(row(['id' => null]), row(['id' => null])));
    }

    public function test_null_is_not_identical_to_value(): void
    {
        static::assertFalse((new Identical('id', 'id'))->compare(row(['id' => null]), row(['id' => 1])));
        static::assertFalse((new Identical('id', 'id'))->compare(row(['id' => 1]), row(['id' => null])));
    }

    public function test_success(): void
    {
        static::assertTrue((new Identical('id', 'id'))->compare(row(['id' => 1]), row(['id' => 1])));
    }
}
