<?php

declare(strict_types=1);

namespace Flow\Dremel\Tests\Unit;

use PHPUnit\Framework\TestCase;

final class DremelTest extends TestCase
{
    public function test_dremel_exception() : void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Independent Dremel implementation is not yet available, please fallback to flow-php/parquet library DremelShredder/DremelAssembler classes');

        new \Flow\Dremel\Dremel();
    }
}
