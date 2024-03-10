<?php

declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit\AccessControl;

use Flow\RDSL\AccessControl\AllowAll;
use PHPUnit\Framework\TestCase;

final class AllowAllTest extends TestCase
{
    public function test_allow_all() : void
    {
        $acl = new AllowAll();

        self::assertTrue($acl->isAllowed('foo'));
        self::assertTrue($acl->isAllowed('bar'));
        self::assertTrue($acl->isAllowed('baz'));
    }
}
