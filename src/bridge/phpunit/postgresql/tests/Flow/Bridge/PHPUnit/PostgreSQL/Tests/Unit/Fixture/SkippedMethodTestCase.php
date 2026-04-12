<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit\Fixture;

use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use PHPUnit\Framework\TestCase;

final class SkippedMethodTestCase extends TestCase
{
    public function test_normal() : void
    {
        $this->addToAssertionCount(1);
    }

    #[SkipTransactionRollback]
    public function test_something() : void
    {
        $this->addToAssertionCount(1);
    }
}
