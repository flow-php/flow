<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Subscriber;

use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use Flow\Bridge\PHPUnit\PostgreSQL\StaticClient;
use PHPUnit\Event\Code\Test;
use PHPUnit\Event\Code\TestMethod;
use PHPUnit\Event\Test\PreparationStarted;
use PHPUnit\Event\Test\PreparationStartedSubscriber;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final readonly class TestPreparationStartedSubscriber implements PreparationStartedSubscriber
{
    public function notify(PreparationStarted $event): void
    {
        StaticClient::rollBack();

        if (self::hasSkipAttribute($event->test())) {
            StaticClient::disable();

            return;
        }

        StaticClient::enable();
        StaticClient::beginTransaction();
    }

    private static function hasSkipAttribute(Test $test): bool
    {
        if (!$test instanceof TestMethod) {
            return false;
        }

        $reflectionClass = new ReflectionClass($test->className());

        if ($reflectionClass->getAttributes(SkipTransactionRollback::class) !== []) {
            return true;
        }

        if (
            $reflectionClass->hasMethod($test->methodName())
            && $reflectionClass->getMethod($test->methodName())->getAttributes(SkipTransactionRollback::class) !== []
        ) {
            return true;
        }

        $parent = $reflectionClass->getParentClass();

        while ($parent !== false && $parent->name !== TestCase::class) {
            if ($parent->getAttributes(SkipTransactionRollback::class) !== []) {
                return true;
            }
            $parent = $parent->getParentClass();
        }

        return false;
    }
}
