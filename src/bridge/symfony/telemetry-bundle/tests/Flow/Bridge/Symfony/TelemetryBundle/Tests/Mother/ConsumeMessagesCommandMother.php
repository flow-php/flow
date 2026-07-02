<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother;

use Psr\Container\ContainerInterface;
use RuntimeException;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\Command\ConsumeMessagesCommand;
use Symfony\Component\Messenger\RoutableMessageBus;

final class ConsumeMessagesCommandMother
{
    public static function create(): ConsumeMessagesCommand
    {
        $locator = new class implements ContainerInterface {
            public function get(string $id): mixed
            {
                throw new RuntimeException('empty locator');
            }

            public function has(string $id): bool
            {
                return false;
            }
        };

        return new ConsumeMessagesCommand(new RoutableMessageBus($locator), $locator, new EventDispatcher());
    }
}
