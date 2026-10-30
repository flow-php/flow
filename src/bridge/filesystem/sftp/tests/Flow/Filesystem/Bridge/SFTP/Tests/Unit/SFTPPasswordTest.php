<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\SFTPPassword;

final class SFTPPasswordTest extends FlowTestCase
{
    public function test_normalize_carries_the_password(): void
    {
        static::assertSame(['password' => 'secret'], (new SFTPPassword('secret'))->normalize());
    }
}
