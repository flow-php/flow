<?php

declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit\AccessControl;

use Flow\RDSL\AccessControl\DenyAll;
use PHPUnit\Framework\TestCase;

final class DenyAllTest extends TestCase
{
    public function test_deny_all() : void
    {
        $acl = new DenyAll();

        self::assertFalse($acl->isAllowed('foo'));
        self::assertFalse($acl->isAllowed('bar'));
        self::assertFalse($acl->isAllowed('baz'));
    }
}
