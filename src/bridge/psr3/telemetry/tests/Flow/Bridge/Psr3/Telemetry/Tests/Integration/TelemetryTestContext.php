<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\Tests\Integration;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\{Logger, LoggerProvider};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidLogExporter;
use Flow\Telemetry\Resource;

final readonly class TelemetryTestContext
{
    public function __construct(
        public MemoryLogProcessor $processor,
        public Logger $logger,
    ) {
    }

    public static function create(
        ?Resource $resource = null,
        string $scope = 'psr3-test-app',
        string $version = 'unknown',
    ) : self {
        $processor = new MemoryLogProcessor(new VoidLogExporter());

        $logger = (new LoggerProvider(
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
        ))->logger(
            $resource ?? Resource::create(['service.name' => 'psr3-test-service']),
            $scope,
            $version,
        );

        return new self($processor, $logger);
    }
}
