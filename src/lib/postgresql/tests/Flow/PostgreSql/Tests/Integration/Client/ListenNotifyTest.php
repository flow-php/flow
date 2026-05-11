<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Client\Notification;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\notify;

final class ListenNotifyTest extends PostgreSqlTestCase
{
    public function test_blocking_wait_unblocks_on_concurrent_notify(): void
    {
        $listener = $this->pgsqlContext()->client;
        $listener->listen('flow_test_blocking');

        $this->pgsqlContext()->spawnBackgroundNotifier('flow_test_blocking', 'delivered', 200);

        $startNs = \hrtime(true);
        $notification = $listener->wait(3000);
        $elapsedMs = (\hrtime(true) - $startNs) / 1_000_000;

        if ($notification === null) {
            $stderr = \implode("\n---\n", $this->pgsqlContext()->backgroundStderrContents());
            static::fail('No notification received. Background sender stderr:' . "\n" . $stderr);
        }

        static::assertSame('flow_test_blocking', $notification->channel);
        static::assertSame('delivered', $notification->payload);
        static::assertGreaterThanOrEqual(150, $elapsedMs);
        static::assertLessThan(2000, $elapsedMs);

        $listener->unlisten('flow_test_blocking');
    }

    public function test_immediate_notification_is_returned_on_non_blocking_check(): void
    {
        $listener = $this->pgsqlContext()->client;
        $sender = $this->pgsqlContext()->newClient();

        $listener->listen('flow_test_channel_immediate');
        $sender->execute(notify('flow_test_channel_immediate')->withPayload('immediate'));

        \usleep(100_000);

        $notification = $listener->wait(0);

        static::assertInstanceOf(Notification::class, $notification);
        static::assertSame('flow_test_channel_immediate', $notification->channel);
        static::assertSame('immediate', $notification->payload);

        $listener->unlisten('flow_test_channel_immediate');
    }

    public function test_listen_receives_notify_from_separate_connection(): void
    {
        $listener = $this->pgsqlContext()->client;
        $sender = $this->pgsqlContext()->newClient();

        $listener->listen('flow_test_channel');
        $sender->execute(notify('flow_test_channel')->withPayload('hello'));

        $notification = $listener->wait(2000);

        static::assertInstanceOf(Notification::class, $notification);
        static::assertSame('flow_test_channel', $notification->channel);
        static::assertSame('hello', $notification->payload);
        static::assertGreaterThan(0, $notification->pid);

        $listener->unlisten('flow_test_channel');
    }

    public function test_listening_multiple_channels(): void
    {
        $listener = $this->pgsqlContext()->client;
        $sender = $this->pgsqlContext()->newClient();

        $listener->listen('flow_test_channel_a');
        $listener->listen('flow_test_channel_b');

        $sender->execute(notify('flow_test_channel_a')->withPayload('from_a'));
        $sender->execute(notify('flow_test_channel_b')->withPayload('from_b'));

        \usleep(100_000);

        $received = [];
        $first = $listener->wait(500);
        $second = $listener->wait(500);

        if ($first !== null) {
            $received[$first->channel] = $first->payload;
        }

        if ($second !== null) {
            $received[$second->channel] = $second->payload;
        }

        static::assertCount(2, $received);
        static::assertSame('from_a', $received['flow_test_channel_a']);
        static::assertSame('from_b', $received['flow_test_channel_b']);

        $listener->unlisten('flow_test_channel_a');
        $listener->unlisten('flow_test_channel_b');
    }

    public function test_notification_empty_payload_is_returned_as_empty_string(): void
    {
        $listener = $this->pgsqlContext()->client;
        $sender = $this->pgsqlContext()->newClient();

        $listener->listen('flow_test_channel_empty');
        $sender->execute(notify('flow_test_channel_empty'));

        $notification = $listener->wait(1000);

        static::assertInstanceOf(Notification::class, $notification);
        static::assertSame('', $notification->payload);

        $listener->unlisten('flow_test_channel_empty');
    }

    public function test_unlisten_stops_receiving_notifications(): void
    {
        $listener = $this->pgsqlContext()->client;
        $sender = $this->pgsqlContext()->newClient();

        $listener->listen('flow_test_channel_unsub');
        $listener->unlisten('flow_test_channel_unsub');

        $sender->execute(notify('flow_test_channel_unsub')->withPayload('should_be_ignored'));

        \usleep(100_000);

        static::assertNull($listener->wait(300));
    }

    public function test_wait_for_notification_negative_timeout_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->pgsqlContext()->client->wait(-1);
    }

    public function test_wait_for_notification_times_out_when_no_message_arrives(): void
    {
        $listener = $this->pgsqlContext()->client;
        $listener->listen('flow_test_channel_timeout');

        $startNs = \hrtime(true);
        $result = $listener->wait(300);
        $elapsedMs = (\hrtime(true) - $startNs) / 1_000_000;

        static::assertNull($result);
        static::assertGreaterThanOrEqual(290, $elapsedMs);
        static::assertLessThan(1000, $elapsedMs);

        $listener->unlisten('flow_test_channel_timeout');
    }
}
