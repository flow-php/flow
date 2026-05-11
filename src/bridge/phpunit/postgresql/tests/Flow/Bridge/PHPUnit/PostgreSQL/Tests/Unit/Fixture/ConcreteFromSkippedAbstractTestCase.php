<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit\Fixture;

final class ConcreteFromSkippedAbstractTestCase extends AbstractSkippedTestCase
{
    public function test_something(): void
    {
        $this->addToAssertionCount(1);
    }
}
