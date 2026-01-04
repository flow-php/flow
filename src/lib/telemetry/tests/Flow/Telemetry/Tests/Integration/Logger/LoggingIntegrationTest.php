<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Logger;

use Flow\Telemetry\Context\{Context, MemoryContextStorage, SpanId, TraceId};
use Flow\Telemetry\Logger\{LogRecord, LoggerProvider, Severity};
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidLogExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

final class LoggingIntegrationTest extends TestCase
{
    private ClockInterface $clock;

    private Resource $resource;

    protected function setUp() : void
    {
        $this->clock = $this->createMock(ClockInterface::class);
        $this->clock->method('now')->willReturn(new \DateTimeImmutable('2024-01-01 12:00:00.123456'));
        $this->resource = ResourceMother::default();
    }

    public function test_complete_logging_workflow() : void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, $this->clock, new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'my-service', '1.0.0');

        $logger->info('Application started', ['environment' => 'production']);
        $logger->debug('Loading configuration');
        $logger->warn('Deprecated feature used', ['feature' => 'legacy-api']);

        try {
            throw new \RuntimeException('Database connection failed');
        } catch (\Throwable $e) {
            $logger->emit(
                (new LogRecord())
                    ->setSeverity(Severity::ERROR)
                    ->setBody('Database error')
                    ->setException($e)
            );
        }

        $logger->info('Application shutdown');

        self::assertCount(5, $processor->entries());
        self::assertCount(2, $processor->entriesWithSeverity(Severity::INFO));
        self::assertCount(1, $processor->entriesWithSeverity(Severity::ERROR));
        self::assertCount(1, $processor->entriesContaining('Database'));

        $errorEntry = $processor->entriesWithSeverity(Severity::ERROR)[0];
        self::assertSame(\RuntimeException::class, $errorEntry->record->attributes->get('exception.type'));
        self::assertSame('Database connection failed', $errorEntry->record->attributes->get('exception.message'));
    }

    public function test_filtering_logs_by_severity_and_content() : void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, $this->clock, new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'test', '1.0');

        $logger->trace('Very detailed trace');
        $logger->debug('Debug information');
        $logger->info('User logged in');
        $logger->info('User logged out');
        $logger->warn('Session expiring soon');
        $logger->error('Authentication failed');
        $logger->fatal('System crash');

        self::assertCount(7, $processor->entries());
        self::assertCount(2, $processor->entriesWithSeverity(Severity::INFO));
        self::assertCount(2, $processor->entriesContaining('logged'));
    }

    public function test_multiple_services_logging() : void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, $this->clock, new MemoryContextStorage());

        $httpLogger = $provider->logger($this->resource, 'http-server', '1.0.0');
        $dbLogger = $provider->logger($this->resource, 'database', '2.0.0');
        $cacheLogger = $provider->logger($this->resource, 'cache', '1.5.0');

        $httpLogger->info('Received request', ['path' => '/api/users', 'method' => 'GET']);
        $cacheLogger->debug('Cache miss', ['key' => 'users:list']);
        $dbLogger->info('Query executed', ['query' => 'SELECT * FROM users', 'duration_ms' => 45]);
        $cacheLogger->info('Cache updated', ['key' => 'users:list']);
        $httpLogger->info('Response sent', ['status' => 200, 'duration_ms' => 52]);

        self::assertCount(5, $processor->entries());
        self::assertCount(4, $processor->entriesWithSeverity(Severity::INFO));
        self::assertCount(1, $processor->entriesWithSeverity(Severity::DEBUG));
    }

    public function test_processor_reset_clears_logs() : void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, $this->clock, new MemoryContextStorage());
        $logger = $provider->logger($this->resource, 'test', '1.0');

        $logger->info('Message 1');
        $logger->info('Message 2');

        self::assertCount(2, $processor->entries());

        $processor->reset();

        self::assertCount(0, $processor->entries());

        $logger->info('Message 3');

        self::assertCount(1, $processor->entries());
    }

    public function test_provider_creates_new_logger_each_time() : void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, $this->clock, new MemoryContextStorage());

        $logger1 = $provider->logger($this->resource, 'service-a', '1.0');
        $logger2 = $provider->logger($this->resource, 'service-a', '1.0');

        self::assertNotSame($logger1, $logger2);

        $logger1->info('From logger 1');
        $logger2->info('From logger 2');

        self::assertCount(2, $processor->entries());
    }

    public function test_span_context_correlation() : void
    {
        $traceId = TraceId::generate();
        $spanId = SpanId::generate();

        $contextStorage = new MemoryContextStorage();
        $context = Context::withTraceId($traceId)->withActiveSpan($spanId);
        $contextStorage->store($context);

        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, $this->clock, $contextStorage);

        $logger = $provider->logger($this->resource, 'my-service', '1.0');
        $logger->info('Request processed');

        $entry = $processor->entries()[0];

        self::assertNotNull($entry->spanContext);
        self::assertSame($traceId->toHex(), $entry->spanContext->traceId->toHex());
        self::assertSame($spanId->toHex(), $entry->spanContext->spanId->toHex());
    }

    private function createProcessor() : MemoryLogProcessor
    {
        return new MemoryLogProcessor(new VoidLogExporter());
    }
}
