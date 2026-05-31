<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Mother;

use PHPUnit\Event\Code\TestDox;
use PHPUnit\Event\Code\TestMethod;
use PHPUnit\Event\Telemetry\Duration;
use PHPUnit\Event\Telemetry\GarbageCollectorStatus;
use PHPUnit\Event\Telemetry\HRTime;
use PHPUnit\Event\Telemetry\Info;
use PHPUnit\Event\Telemetry\MemoryUsage;
use PHPUnit\Event\Telemetry\Snapshot;
use PHPUnit\Event\Test\Finished;
use PHPUnit\Event\Test\PreparationStarted;
use PHPUnit\Event\TestData\TestDataCollection;
use PHPUnit\Metadata\MetadataCollection;

final class TestEventMother
{
    /**
     * @param class-string $className
     * @param non-empty-string $methodName
     */
    public static function finished(string $className = self::class, string $methodName = 'test_example'): Finished
    {
        return new Finished(self::createInfo(), self::createTestMethod($className, $methodName), 1);
    }

    /**
     * @param class-string $className
     * @param non-empty-string $methodName
     */
    public static function preparationStarted(
        string $className = self::class,
        string $methodName = 'test_example',
    ): PreparationStarted {
        return new PreparationStarted(self::createInfo(), self::createTestMethod($className, $methodName));
    }

    private static function createInfo(): Info
    {
        return new Info(
            new Snapshot(
                HRTime::fromSecondsAndNanoseconds(0, 0),
                MemoryUsage::fromBytes(0),
                MemoryUsage::fromBytes(0),
                new GarbageCollectorStatus(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, false, false, false, 0),
            ),
            Duration::fromSecondsAndNanoseconds(0, 0),
            MemoryUsage::fromBytes(0),
            Duration::fromSecondsAndNanoseconds(0, 0),
            MemoryUsage::fromBytes(0),
        );
    }

    /**
     * @param class-string $className
     * @param non-empty-string $methodName
     */
    private static function createTestMethod(string $className, string $methodName): TestMethod
    {
        return new TestMethod(
            $className,
            $methodName,
            'tests/' . $className . '.php',
            10,
            new TestDox($className, $methodName, $methodName),
            MetadataCollection::fromArray([]),
            TestDataCollection::fromArray([]),
        );
    }
}
