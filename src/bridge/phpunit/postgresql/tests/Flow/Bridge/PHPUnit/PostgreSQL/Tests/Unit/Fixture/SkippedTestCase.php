<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit\Fixture;

use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use PHPUnit\Framework\TestCase;

#[SkipTransactionRollback]
final class SkippedTestCase extends TestCase
{
    public function test_something(): void
    {
        $this->addToAssertionCount(1);
    }
}
