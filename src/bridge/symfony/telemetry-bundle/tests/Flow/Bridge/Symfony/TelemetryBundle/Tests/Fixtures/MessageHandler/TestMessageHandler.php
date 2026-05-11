<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
final class TestMessageHandler
{
    public bool $handled = false;

    public function __invoke(TestMessage $message): void
    {
        $this->handled = true;
    }
}
