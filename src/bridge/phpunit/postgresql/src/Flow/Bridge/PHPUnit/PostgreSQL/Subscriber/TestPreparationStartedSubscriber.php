<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Subscriber;

use Flow\Bridge\PHPUnit\PostgreSQL\{SkipTransactionRollback, StaticClient};
use PHPUnit\Event\Code\{Test, TestMethod};
use PHPUnit\Event\Test\{PreparationStarted, PreparationStartedSubscriber};
use PHPUnit\Framework\TestCase;

final readonly class TestPreparationStartedSubscriber implements PreparationStartedSubscriber
{
    public function notify(PreparationStarted $event) : void
    {
        StaticClient::rollBack();

        if (self::hasSkipAttribute($event->test())) {
            StaticClient::disable();

            return;
        }

        StaticClient::enable();
        StaticClient::beginTransaction();
    }

    private static function hasSkipAttribute(Test $test) : bool
    {
        if (!$test instanceof TestMethod) {
            return false;
        }

        $reflectionClass = new \ReflectionClass($test->className());

        if ($reflectionClass->getAttributes(SkipTransactionRollback::class)) {
            return true;
        }

        if ($reflectionClass->hasMethod($test->methodName())
            && $reflectionClass->getMethod($test->methodName())->getAttributes(SkipTransactionRollback::class)
        ) {
            return true;
        }

        while (($reflectionClass = $reflectionClass->getParentClass())
            && $reflectionClass->name !== TestCase::class
        ) {
            if ($reflectionClass->getAttributes(SkipTransactionRollback::class)) {
                return true;
            }
        }

        return false;
    }
}
