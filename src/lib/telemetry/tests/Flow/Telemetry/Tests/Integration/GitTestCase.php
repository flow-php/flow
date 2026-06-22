<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration;

use Flow\Telemetry\Tests\Context\GitContext;
use PHPUnit\Framework\TestCase;

abstract class GitTestCase extends TestCase
{
    protected GitContext $gitContext;

    protected function setUp(): void
    {
        $this->gitContext = new GitContext();

        if (!$this->gitContext->gitBinaryExists()) {
            static::markTestSkipped('Git binary is unavailable');
        }
    }

    protected function tearDown(): void
    {
        $this->gitContext->cleanup();
    }
}
