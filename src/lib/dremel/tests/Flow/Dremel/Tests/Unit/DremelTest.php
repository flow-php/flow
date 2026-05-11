<?php

declare(strict_types=1);

namespace Flow\Dremel\Tests\Unit;

use PHPUnit\Framework\TestCase;

final class DremelTest extends TestCase
{
    public function test_dremel_exception(): void
    {
        static::markTestSkipped('Dremel is not yet available as a standalone package');
    }
}
