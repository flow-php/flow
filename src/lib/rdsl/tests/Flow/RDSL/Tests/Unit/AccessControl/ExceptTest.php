<?php

declare(strict_types=1);

namespace Flow\RDSL\Tests\Unit\AccessControl;

use Flow\RDSL\AccessControl\{AllowList, Except};
use PHPUnit\Framework\TestCase;

final class ExceptTest extends TestCase
{
    public function test_except() : void
    {
        $acl = new Except(
            new AllowList(['foo', 'bar']),
            ['bar']
        );

        self::assertTrue($acl->isAllowed('foo'));
        self::assertFalse($acl->isAllowed('bar'));
        self::assertFalse($acl->isAllowed('baz'));
    }
}
