<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Detector\CachingDetector;
use Flow\Telemetry\Resource\ResourceDetector;
use PHPUnit\Framework\TestCase;

final class CachingDetectorTest extends TestCase
{
    private string $cacheFile;

    protected function setUp() : void
    {
        $this->cacheFile = \sys_get_temp_dir() . '/flow_telemetry_test_' . \uniqid() . '.cache';
    }

    protected function tearDown() : void
    {
        if (\is_file($this->cacheFile)) {
            @\unlink($this->cacheFile);
        }
    }

    public function test_detect_caches_result_to_file() : void
    {
        $innerDetector = $this->createMockDetector(['key' => 'value']);
        $detector = new CachingDetector($innerDetector, $this->cacheFile);

        $detector->detect();

        self::assertFileExists($this->cacheFile);
    }

    public function test_detect_calls_inner_detector_when_cache_missing() : void
    {
        $callCount = 0;
        $innerDetector = new class($callCount) implements ResourceDetector {
            public function __construct(private int &$callCount)
            {
            }

            public function detect() : Resource
            {
                $this->callCount++;

                return Resource::create(['key' => 'value']);
            }
        };

        $detector = new CachingDetector($innerDetector, $this->cacheFile);
        $detector->detect();

        self::assertSame(1, $callCount);
    }

    public function test_detect_handles_corrupted_cache_gracefully() : void
    {
        \file_put_contents($this->cacheFile, 'invalid-serialized-data');

        $innerDetector = $this->createMockDetector(['key' => 'fresh-value']);
        $detector = new CachingDetector($innerDetector, $this->cacheFile);

        $resource = $detector->detect();

        self::assertSame('fresh-value', $resource->get('key'));
    }

    public function test_detect_returns_cached_result_without_calling_inner_detector() : void
    {
        $callCount = 0;
        $innerDetector = new class($callCount) implements ResourceDetector {
            public function __construct(private int &$callCount)
            {
            }

            public function detect() : Resource
            {
                $this->callCount++;

                return Resource::create(['key' => 'value']);
            }
        };

        $detector = new CachingDetector($innerDetector, $this->cacheFile);

        $detector->detect();
        $detector->detect();

        self::assertSame(1, $callCount);
    }

    public function test_detect_returns_correct_resource_from_cache() : void
    {
        $innerDetector = $this->createMockDetector([
            'service.name' => 'test-service',
            'host.name' => 'test-host',
        ]);

        $detector = new CachingDetector($innerDetector, $this->cacheFile);

        $firstResult = $detector->detect();
        $secondResult = $detector->detect();

        self::assertSame('test-service', $firstResult->get('service.name'));
        self::assertSame('test-host', $firstResult->get('host.name'));
        self::assertSame('test-service', $secondResult->get('service.name'));
        self::assertSame('test-host', $secondResult->get('host.name'));
    }

    public function test_detect_uses_default_cache_path_when_not_provided() : void
    {
        $defaultCachePath = \sys_get_temp_dir() . '/flow_telemetry_resource.cache';

        if (\is_file($defaultCachePath)) {
            @\unlink($defaultCachePath);
        }

        $innerDetector = $this->createMockDetector(['key' => 'value']);
        $detector = new CachingDetector($innerDetector);

        $detector->detect();

        self::assertFileExists($defaultCachePath);

        @\unlink($defaultCachePath);
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
