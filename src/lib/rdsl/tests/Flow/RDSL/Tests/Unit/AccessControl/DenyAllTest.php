<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit\AccessControl;

use Flow\RDSL\AccessControl\DenyAll;
use PHPUnit\Framework\TestCase;

final class DenyAllTest extends TestCase
{
    public function test_deny_all() : void
    {
        $acl = new DenyAll();

        $this->assertFalse($acl->isAllowed('foo'));
        $this->assertFalse($acl->isAllowed('bar'));
        $this->assertFalse($acl->isAllowed('baz'));
    }
}
