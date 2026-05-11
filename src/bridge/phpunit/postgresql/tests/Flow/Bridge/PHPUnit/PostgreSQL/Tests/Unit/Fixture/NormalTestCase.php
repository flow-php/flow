<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit\Fixture;

use PHPUnit\Framework\TestCase;

final class NormalTestCase extends TestCase
{
    public function test_something(): void
    {
        $this->addToAssertionCount(1);
    }
}
