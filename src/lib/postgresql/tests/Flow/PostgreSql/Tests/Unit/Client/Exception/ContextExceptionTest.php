<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\ContextException;
use PHPUnit\Framework\TestCase;

final class ContextExceptionTest extends TestCase
{
    public function test_key_not_found_message() : void
    {
        self::assertStringContainsString('Context has no value for key "foo".', ContextException::keyNotFound('foo')->getMessage());
    }
}
