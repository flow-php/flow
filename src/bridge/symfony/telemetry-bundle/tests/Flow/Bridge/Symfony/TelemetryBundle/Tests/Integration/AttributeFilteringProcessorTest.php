<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Logger\Middleware\AttributeFilteringLogMiddleware;
use Flow\Telemetry\Logger\Processor\BatchingLogProcessor;
use Flow\Telemetry\Logger\Processor\PipelineLogProcessor;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\Processor\AttributeFilteringMetricProcessor;
use Flow\Telemetry\Tracer\Processor\AttributeFilteringSpanProcessor;

use function count;
use function glob;
use function is_string;

final class AttributeFilteringProcessorTest extends KernelTestCase
{
    private const OTLP = [
        'otlp' => ['otlp' => ['transport' => ['type' => 'curl', 'endpoint' => 'http://localhost:4318']]],
    ];

    public function test_log_pipeline_wraps_attribute_filtering_middleware_and_sink(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    'matcher' => ['path' => 'http.route', 'mode' => 'equal', 'value' => '/health'],
                                ],
                            ],
                            'sink' => ['type' => 'batching', 'exporter' => 'otlp'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(
            PipelineLogProcessor::class,
            $container->get('flow.telemetry.logger_provider.processor'),
        );
        static::assertInstanceOf(
            AttributeFilteringLogMiddleware::class,
            $container->get('flow.telemetry.logger_provider.processor.middleware.0'),
        );
        static::assertInstanceOf(
            BatchingLogProcessor::class,
            $container->get('flow.telemetry.logger_provider.processor.sink.processor'),
        );
    }

    public function test_metric_processor_is_attribute_filtering(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'attribute_filtering',
                            'matcher' => ['path' => 'endpoint', 'mode' => 'contains', 'value' => '/internal/'],
                            'inner_processor' => ['type' => 'batching', 'exporter' => 'otlp'],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(
            AttributeFilteringMetricProcessor::class,
            $this->getContainer()->get('flow.telemetry.meter_provider.processor'),
        );
    }

    public function test_span_processor_is_attribute_filtering(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'attribute_filtering',
                            'matcher' => ['path' => 'http.route', 'mode' => 'equal', 'value' => '/health'],
                            'inner_processor' => ['type' => 'batching', 'exporter' => 'otlp'],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(
            AttributeFilteringSpanProcessor::class,
            $this->getContainer()->get('flow.telemetry.tracer_provider.processor'),
        );
    }

    public function test_filter_drops_matching_and_keeps_non_matching(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    'matcher' => [
                                        'any' => [
                                            ['path' => 'http.route', 'mode' => 'equal', 'value' => '/health'],
                                            [
                                                'path' => 'user_agent',
                                                'mode' => 'contains',
                                                'value' => 'bot',
                                                'case_sensitive' => false,
                                            ],
                                        ],
                                    ],
                                ],
                            ],
                            'sink' => ['type' => 'void'],
                        ],
                    ],
                ]);
            },
        ]);

        $filter = $this->getContainer()->get('flow.telemetry.logger_provider.processor.middleware.0.filter');
        static::assertInstanceOf(AttributeFilter::class, $filter);

        static::assertTrue($filter->shouldDrop(Attributes::create(['http.route' => '/health'])));
        static::assertTrue($filter->shouldDrop(Attributes::create(['user_agent' => 'GoogleBot/2.1'])));
        static::assertFalse($filter->shouldDrop(Attributes::create(['http.route' => '/api'])));
    }

    public function test_exclude_false_keeps_only_matching(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    'exclude' => false,
                                    'matcher' => ['path' => 'keep', 'mode' => 'equal', 'value' => true],
                                ],
                            ],
                            'sink' => ['type' => 'void'],
                        ],
                    ],
                ]);
            },
        ]);

        $filter = $this->getContainer()->get('flow.telemetry.logger_provider.processor.middleware.0.filter');
        static::assertInstanceOf(AttributeFilter::class, $filter);

        static::assertFalse($filter->shouldDrop(Attributes::create(['keep' => true])));
        static::assertTrue($filter->shouldDrop(Attributes::create(['keep' => false])));
    }

    public function test_sources_are_configured(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    'sources' => ['resource', 'scope'],
                                    'matcher' => ['path' => 'service.name', 'mode' => 'equal', 'value' => 'x'],
                                ],
                            ],
                            'sink' => ['type' => 'void'],
                        ],
                    ],
                ]);
            },
        ]);

        $filter = $this->getContainer()->get('flow.telemetry.logger_provider.processor.middleware.0.filter');
        static::assertInstanceOf(AttributeFilter::class, $filter);
        static::assertSame([AttributeSource::RESOURCE, AttributeSource::SCOPE], $filter->sources());
    }

    public function test_all_with_negated_rule_and_nested_path(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    'matcher' => [
                                        'all' => [
                                            ['path' => ['timing', 'duration_ms'], 'mode' => 'less_than', 'value' => 5],
                                            ['not' => ['path' => 'env', 'mode' => 'equal', 'value' => 'prod']],
                                        ],
                                    ],
                                ],
                            ],
                            'sink' => ['type' => 'void'],
                        ],
                    ],
                ]);
            },
        ]);

        $filter = $this->getContainer()->get('flow.telemetry.logger_provider.processor.middleware.0.filter');
        static::assertInstanceOf(AttributeFilter::class, $filter);

        // all: duration < 5 AND NOT(env = prod)
        static::assertTrue($filter->shouldDrop(Attributes::create(['timing' => ['duration_ms' => 2], 'env' => 'dev'])));
        static::assertFalse($filter->shouldDrop(Attributes::create([
            'timing' => ['duration_ms' => 2],
            'env' => 'prod',
        ])));
        static::assertFalse($filter->shouldDrop(Attributes::create([
            'timing' => ['duration_ms' => 9],
            'env' => 'dev',
        ])));
    }

    public function test_deeply_nested_matcher_combines_ands_and_ors(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    // (tenant = a AND audit = true) OR (severity >= 5 AND NOT path ^= /health)
                                    'matcher' => [
                                        'any' => [
                                            [
                                                'all' => [
                                                    ['path' => 'tenant', 'mode' => 'equal', 'value' => 'a'],
                                                    ['path' => 'audit', 'mode' => 'equal', 'value' => true],
                                                ],
                                            ],
                                            [
                                                'all' => [
                                                    [
                                                        'path' => 'severity',
                                                        'mode' => 'greater_than_equal',
                                                        'value' => 5,
                                                    ],
                                                    ['not' => [
                                                        'path' => 'path',
                                                        'mode' => 'starts_with',
                                                        'value' => '/health',
                                                    ]],
                                                ],
                                            ],
                                        ],
                                    ],
                                ],
                            ],
                            'sink' => ['type' => 'void'],
                        ],
                    ],
                ]);
            },
        ]);

        $filter = $this->getContainer()->get('flow.telemetry.logger_provider.processor.middleware.0.filter');
        static::assertInstanceOf(AttributeFilter::class, $filter);

        // First branch: tenant = a AND audit = true
        static::assertTrue($filter->shouldDrop(Attributes::create(['tenant' => 'a', 'audit' => true])));
        // Second branch: severity >= 5 AND path is not under /health
        static::assertTrue($filter->shouldDrop(Attributes::create(['severity' => 7, 'path' => '/api'])));
        // Second branch fails: severity high but path is under /health
        static::assertFalse($filter->shouldDrop(Attributes::create(['severity' => 7, 'path' => '/health/live'])));
        // Neither branch: tenant matches but audit missing, severity below threshold
        static::assertFalse($filter->shouldDrop(Attributes::create([
            'tenant' => 'a',
            'severity' => 1,
            'path' => '/api',
        ])));
    }

    public function test_per_channel_severity_thresholds_drive_the_wired_middleware(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    // Keep ERROR+ from payments, but DEBUG+ from importer.
                                    'exclude' => false,
                                    'matcher' => [
                                        'any' => [
                                            [
                                                'all' => [
                                                    [
                                                        'path' => 'flow.log.channel',
                                                        'mode' => 'equal',
                                                        'value' => 'payments',
                                                    ],
                                                    [
                                                        'path' => AttributeFilteringLogMiddleware::SEVERITY_KEY,
                                                        'mode' => 'greater_than_equal',
                                                        'value' => Severity::ERROR->value,
                                                    ],
                                                ],
                                            ],
                                            [
                                                'all' => [
                                                    [
                                                        'path' => 'flow.log.channel',
                                                        'mode' => 'equal',
                                                        'value' => 'importer',
                                                    ],
                                                    [
                                                        'path' => AttributeFilteringLogMiddleware::SEVERITY_KEY,
                                                        'mode' => 'greater_than_equal',
                                                        'value' => Severity::DEBUG->value,
                                                    ],
                                                ],
                                            ],
                                        ],
                                    ],
                                ],
                            ],
                            'sink' => ['type' => 'void'],
                        ],
                    ],
                ]);
            },
        ]);

        $middleware = $this->getContainer()->get('flow.telemetry.logger_provider.processor.middleware.0');
        static::assertInstanceOf(AttributeFilteringLogMiddleware::class, $middleware);

        $paymentsError = LogEntryMother::onChannel(Severity::ERROR, 'payments');
        $importerDebug = LogEntryMother::onChannel(Severity::DEBUG, 'importer');

        static::assertSame($paymentsError, $middleware->process($paymentsError));
        static::assertSame($importerDebug, $middleware->process($importerDebug));
        static::assertNull($middleware->process(LogEntryMother::onChannel(Severity::INFO, 'payments')));
        static::assertNull($middleware->process(LogEntryMother::onChannel(Severity::TRACE, 'importer')));
    }

    public function test_matcher_compiles_into_the_kernel_cache_dir(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => self::OTLP,
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'pipeline',
                            'middleware' => [
                                [
                                    'type' => 'attribute_filtering',
                                    'matcher' => ['path' => 'http.route', 'mode' => 'equal', 'value' => '/health'],
                                ],
                            ],
                            'sink' => ['type' => 'void'],
                        ],
                    ],
                ]);
            },
        ]);

        $filter = $this->getContainer()->get('flow.telemetry.logger_provider.processor.middleware.0.filter');
        static::assertInstanceOf(AttributeFilter::class, $filter);
        $filter->shouldDrop(Attributes::create([])); // triggers code generation

        $cacheDir = $this->getContainer()->getParameter('kernel.cache_dir');
        static::assertTrue(is_string($cacheDir));
        $files = glob($cacheDir . '/flow_telemetry_filters/flow_telemetry_filter_*.php');
        static::assertNotFalse($files);
        static::assertGreaterThan(0, count($files));
    }
}
