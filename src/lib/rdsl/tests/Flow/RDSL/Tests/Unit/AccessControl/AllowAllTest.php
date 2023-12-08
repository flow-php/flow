<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit\AccessControl;

use Flow\RDSL\AccessControl\AllowAll;
use PHPUnit\Framework\TestCase;

final class AllowAllTest extends TestCase
{
    public function test_allow_all() : void
    {
        $acl = new AllowAll();

        $this->assertTrue($acl->isAllowed('foo'));
        $this->assertTrue($acl->isAllowed('bar'));
        $this->assertTrue($acl->isAllowed('baz'));
    }
}
