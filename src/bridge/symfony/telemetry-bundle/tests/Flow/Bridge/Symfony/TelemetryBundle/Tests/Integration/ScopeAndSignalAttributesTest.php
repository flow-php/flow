<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Tracer;

final class ScopeAndSignalAttributesTest extends KernelTestCase
{
    private const MEMORY = ['memory' => ['memory' => null], 'void' => ['void' => null]];

    public function test_logger_scope_and_signal_attributes_are_applied(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::MEMORY,
                    'logger_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'loggers' => [
                        'app' => [
                            'attributes' => [
                                'scope' => ['service.component' => 'billing'],
                                'signal' => ['env' => 'prod', 'region' => 'eu'],
                            ],
                        ],
                    ],
                    'instrumentation' => ['http_kernel' => false, 'console' => false, 'messenger' => false],
                ]);
            },
        ]);

        $logger = $this->getContainer()->get('flow.telemetry.app.logger');
        static::assertInstanceOf(Logger::class, $logger);

        $logger->info('hello', ['env' => 'dev', 'request.id' => 'abc']);

        $processor = $this->getContainer()->get('flow.telemetry.logger_provider.processor');
        static::assertInstanceOf(MemoryLogProcessor::class, $processor);

        $entry = $processor->entries()[0];
        static::assertSame('dev', $entry->record->attributes->get('env'));
        static::assertSame('eu', $entry->record->attributes->get('region'));
        static::assertSame('abc', $entry->record->attributes->get('request.id'));
        static::assertSame('billing', $entry->scope->attributes->get('service.component'));
    }

    public function test_meter_scope_and_signal_attributes_are_applied(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::MEMORY,
                    'meter_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'meters' => [
                        'app' => [
                            'attributes' => [
                                'scope' => ['service.component' => 'billing'],
                                'signal' => ['env' => 'prod', 'region' => 'eu'],
                            ],
                        ],
                    ],
                    'instrumentation' => ['http_kernel' => false, 'console' => false, 'messenger' => false],
                ]);
            },
        ]);

        $meter = $this->getContainer()->get('flow.telemetry.app.meter');
        static::assertInstanceOf(Meter::class, $meter);

        $meter->createCounter('requests')->add(1, ['env' => 'dev']);

        $telemetry = $this->getContainer()->get('flow.telemetry');
        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->flush();

        $processor = $this->getContainer()->get('flow.telemetry.meter_provider.processor');
        static::assertInstanceOf(MemoryMetricProcessor::class, $processor);

        $metric = $processor->metricsWithName('requests')[0];
        static::assertSame('dev', $metric->attributes->get('env'));
        static::assertSame('eu', $metric->attributes->get('region'));
        static::assertSame('billing', $metric->scope->attributes->get('service.component'));
    }

    public function test_tracer_scope_and_signal_attributes_are_applied(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::MEMORY,
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'tracers' => [
                        'app' => [
                            'attributes' => [
                                'scope' => ['service.component' => 'billing'],
                                'signal' => ['env' => 'prod', 'region' => 'eu'],
                            ],
                        ],
                    ],
                    'instrumentation' => ['http_kernel' => false, 'console' => false, 'messenger' => false],
                ]);
            },
        ]);

        $tracer = $this->getContainer()->get('flow.telemetry.app.tracer');
        static::assertInstanceOf(Tracer::class, $tracer);

        $span = $tracer->span('operation', attributes: ['env' => 'dev']);

        static::assertSame('dev', $span->attributes()['env']);
        static::assertSame('eu', $span->attributes()['region']);
        static::assertSame('billing', $span->scope()->attributes->get('service.component'));
    }
}
