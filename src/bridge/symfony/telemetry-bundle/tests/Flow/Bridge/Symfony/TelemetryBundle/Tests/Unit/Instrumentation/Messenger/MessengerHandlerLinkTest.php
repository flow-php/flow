<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerHandlerLink;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

#[CoversClass(MessengerHandlerLink::class)]
final class MessengerHandlerLinkTest extends TestCase
{
    #[TestWith(['dispatcher', true, false])]
    #[TestWith(['worker', false, true])]
    #[TestWith(['both', true, true])]
    public function test_link_targets(string $value, bool $linksDispatcher, bool $linksWorker): void
    {
        $link = MessengerHandlerLink::from($value);

        static::assertSame($linksDispatcher, $link->linksDispatcher());
        static::assertSame($linksWorker, $link->linksWorker());
    }
}
