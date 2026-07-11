<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Mother;

use PHPUnit\Event\Code\TestCollection;
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
use PHPUnit\Event\TestSuite\Finished as SuiteFinished;
use PHPUnit\Event\TestSuite\Started as SuiteStarted;
use PHPUnit\Event\TestSuite\TestSuiteWithName;
use PHPUnit\Metadata\MetadataCollection;
use ReflectionClass;
use ReflectionNamedType;

use function count;

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

    /**
     * @param non-empty-string $suiteName
     */
    public static function suiteFinished(string $suiteName = 'Example Suite', int $testCount = 1): SuiteFinished
    {
        return new SuiteFinished(
            self::createInfo(),
            new TestSuiteWithName($suiteName, $testCount, TestCollection::fromArray([])),
        );
    }

    /**
     * @param non-empty-string $suiteName
     */
    public static function suiteStarted(string $suiteName = 'Example Suite', int $testCount = 1): SuiteStarted
    {
        return new SuiteStarted(
            self::createInfo(),
            new TestSuiteWithName($suiteName, $testCount, TestCollection::fromArray([])),
        );
    }

    /**
     * PHPUnit 13 extended Info::__construct() from 5 to 11 arguments, adding trailing CpuTime
     * values.
     */
    private static function createInfo(): Info
    {
        $arguments = [
            self::createSnapshot(),
            Duration::fromSecondsAndNanoseconds(0, 0),
            MemoryUsage::fromBytes(0),
            Duration::fromSecondsAndNanoseconds(0, 0),
            MemoryUsage::fromBytes(0),
        ];

        $constructor = (new ReflectionClass(Info::class))->getConstructor();

        if ($constructor !== null) {
            $parameters = $constructor->getParameters();

            for ($position = count($arguments); $position < count($parameters); $position++) {
                $type = $parameters[$position]->getType();

                if ($type instanceof ReflectionNamedType) {
                    $cpuTime = $type->getName();
                    $arguments[] = $cpuTime::fromSecondsAndNanoseconds(0, 0);
                }
            }
        }

        return new Info(...$arguments);
    }

    /**
     * PHPUnit 13 extended Snapshot::__construct() from 4 to 7 arguments, adding trailing CpuTime
     * values. See createInfo() for the reflective rationale.
     */
    private static function createSnapshot(): Snapshot
    {
        $arguments = [
            HRTime::fromSecondsAndNanoseconds(0, 0),
            MemoryUsage::fromBytes(0),
            MemoryUsage::fromBytes(0),
            new GarbageCollectorStatus(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, false, false, false, 0),
        ];

        $constructor = (new ReflectionClass(Snapshot::class))->getConstructor();

        if ($constructor !== null) {
            $parameters = $constructor->getParameters();

            for ($position = count($arguments); $position < count($parameters); $position++) {
                $type = $parameters[$position]->getType();

                if ($type instanceof ReflectionNamedType) {
                    $cpuTime = $type->getName();
                    $arguments[] = $cpuTime::fromSecondsAndNanoseconds(0, 0);
                }
            }
        }

        return new Snapshot(...$arguments);
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
