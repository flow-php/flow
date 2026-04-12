<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\Notification;
use PHPUnit\Framework\TestCase;

final class NotificationTest extends TestCase
{
    public function test_exposes_channel_payload_and_pid() : void
    {
        $notification = new Notification('my_channel', 'hello', 12345);

        self::assertSame('my_channel', $notification->channel);
        self::assertSame('hello', $notification->payload);
        self::assertSame(12345, $notification->pid);
    }

    public function test_payload_can_be_empty_string() : void
    {
        self::assertSame('', (new Notification('my_channel', '', 1))->payload);
    }
}
