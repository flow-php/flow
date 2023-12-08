<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit\AccessControl;

use Flow\RDSL\AccessControl\AllowList;
use Flow\RDSL\AccessControl\Except;
use PHPUnit\Framework\TestCase;

final class ExceptTest extends TestCase
{
    public function test_except() : void
    {
        $acl = new Except(
            new AllowList(['foo', 'bar']),
            ['bar']
        );

        $this->assertTrue($acl->isAllowed('foo'));
        $this->assertFalse($acl->isAllowed('bar'));
        $this->assertFalse($acl->isAllowed('baz'));
    }
}
