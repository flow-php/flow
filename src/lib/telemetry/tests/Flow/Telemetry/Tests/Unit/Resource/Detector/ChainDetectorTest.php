<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Detector\ChainDetector;
use Flow\Telemetry\Resource\ResourceDetector;
use PHPUnit\Framework\TestCase;

final class ChainDetectorTest extends TestCase
{
    public function test_chain_with_three_detectors_merges_all() : void
    {
        $detector1 = $this->createMockDetector(['a' => '1', 'common' => 'first']);
        $detector2 = $this->createMockDetector(['b' => '2', 'common' => 'second']);
        $detector3 = $this->createMockDetector(['c' => '3', 'common' => 'third']);

        $detector = new ChainDetector($detector1, $detector2, $detector3);
        $resource = $detector->detect();

        self::assertSame('1', $resource->get('a'));
        self::assertSame('2', $resource->get('b'));
        self::assertSame('3', $resource->get('c'));
        self::assertSame('third', $resource->get('common'));
    }

    public function test_detect_executes_detectors_in_order() : void
    {
        $executionOrder = [];

        $detector1 = new class($executionOrder) implements ResourceDetector {
            /**
             * @param array<int> $executionOrder
             *
             * @phpstan-ignore property.onlyWritten
             */
            public function __construct(private array &$executionOrder)
            {
            }

            public function detect() : Resource
            {
                $this->executionOrder[] = 1;

                return Resource::create(['order' => '1']);
            }
        };

        $detector2 = new class($executionOrder) implements ResourceDetector {
            /**
             * @param array<int> $executionOrder
             *
             * @phpstan-ignore property.onlyWritten
             */
            public function __construct(private array &$executionOrder)
            {
            }

            public function detect() : Resource
            {
                $this->executionOrder[] = 2;

                return Resource::create(['order' => '2']);
            }
        };

        $detector = new ChainDetector($detector1, $detector2);
        $resource = $detector->detect();

        self::assertSame([1, 2], $executionOrder);
        self::assertSame('2', $resource->get('order'));
    }

    public function test_detect_handles_empty_resources_gracefully() : void
    {
        $emptyDetector = $this->createMockDetector([]);
        $nonEmptyDetector = $this->createMockDetector(['key' => 'value']);

        $detector = new ChainDetector($emptyDetector, $nonEmptyDetector, $emptyDetector);
        $resource = $detector->detect();

        self::assertSame(1, $resource->count());
        self::assertSame('value', $resource->get('key'));
    }

    public function test_detect_merges_multiple_detectors() : void
    {
        $detector1 = $this->createMockDetector(['key1' => 'value1']);
        $detector2 = $this->createMockDetector(['key2' => 'value2']);

        $detector = new ChainDetector($detector1, $detector2);
        $resource = $detector->detect();

        self::assertSame('value1', $resource->get('key1'));
        self::assertSame('value2', $resource->get('key2'));
    }

    public function test_detect_with_no_detectors_returns_empty_resource() : void
    {
        $detector = new ChainDetector();
        $resource = $detector->detect();

        self::assertTrue($resource->isEmpty());
    }

    public function test_detect_with_single_detector_returns_its_resource() : void
    {
        $mockDetector = $this->createMockDetector(['key' => 'value']);

        $detector = new ChainDetector($mockDetector);
        $resource = $detector->detect();

        self::assertSame('value', $resource->get('key'));
    }

    public function test_later_detectors_override_earlier_ones() : void
    {
        $detector1 = $this->createMockDetector(['key' => 'original', 'only1' => 'first']);
        $detector2 = $this->createMockDetector(['key' => 'override', 'only2' => 'second']);

        $detector = new ChainDetector($detector1, $detector2);
        $resource = $detector->detect();

        self::assertSame('override', $resource->get('key'));
        self::assertSame('first', $resource->get('only1'));
        self::assertSame('second', $resource->get('only2'));
    }

    /**
     * @param array<string, string> $attributes
     */
    private function createMockDetector(array $attributes) : ResourceDetector
    {
        return new class($attributes) implements ResourceDetector {
            /**
             * @param array<string, string> $attributes
             */
            public function __construct(private readonly array $attributes)
            {
            }

            public function detect() : Resource
            {
                return Resource::create($this->attributes);
            }
        };
    }
}
