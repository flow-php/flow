<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig\TracingTwigExtension;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\Attributes\CoversClass;
use Twig\Environment;
use Twig\Extension\AbstractExtension;
use Twig\Loader\ArrayLoader;

#[CoversClass(TracingTwigExtension::class)]
final class TracingTwigExtensionTest extends KernelTestCase
{
    protected function setUp() : void
    {
        if (!\class_exists(AbstractExtension::class)) {
            self::markTestSkipped('twig/twig is not installed');
        }

        parent::setUp();
    }

    public function test_does_not_trace_blocks_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                        'twig' => [
                            'enabled' => true,
                            'trace_blocks' => false,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'base.html.twig' => '{% block content %}Default content{% endblock %}',
            'child.html.twig' => '{% extends "base.html.twig" %}{% block content %}Child content{% endblock %}',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $result = $twig->render('child.html.twig');

        self::assertSame('Child content', $result);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        foreach ($spans as $span) {
            $attributes = $span->attributes();
            self::assertNotSame('block', $attributes['twig.type'] ?? '', 'Block span should not be traced');
        }
    }

    public function test_does_not_trace_excluded_templates() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'twig' => [
                            'enabled' => true,
                            'exclude_templates' => ['excluded.html.twig'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'included.html.twig' => 'Included template',
            'excluded.html.twig' => 'Excluded template',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $twig->render('included.html.twig');
        $twig->render('excluded.html.twig');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $templateNames = [];

        foreach ($spans as $span) {
            $attributes = $span->attributes();

            if (($attributes['twig.type'] ?? '') === 'template') {
                $templateNames[] = $attributes['twig.template'];
            }
        }

        self::assertContains('included.html.twig', $templateNames, 'Included template should be traced');
        self::assertNotContains('excluded.html.twig', $templateNames, 'Excluded template should not be traced');
    }

    public function test_does_not_trace_excluded_templates_with_regex() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'twig' => [
                            'enabled' => true,
                            'exclude_templates' => ['/^@Profiler.*/'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'included.html.twig' => 'Included template',
            '@Profiler/toolbar.html.twig' => 'Profiler toolbar',
            '@Profiler/panel.html.twig' => 'Profiler panel',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $twig->render('included.html.twig');
        $twig->render('@Profiler/toolbar.html.twig');
        $twig->render('@Profiler/panel.html.twig');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $templateNames = [];

        foreach ($spans as $span) {
            $attributes = $span->attributes();

            if (($attributes['twig.type'] ?? '') === 'template') {
                $templateNames[] = $attributes['twig.template'];
            }
        }

        self::assertContains('included.html.twig', $templateNames, 'Included template should be traced');
        self::assertNotContains('@Profiler/toolbar.html.twig', $templateNames, 'Profiler toolbar should not be traced');
        self::assertNotContains('@Profiler/panel.html.twig', $templateNames, 'Profiler panel should not be traced');
    }

    public function test_does_not_trace_templates_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'twig' => [
                            'enabled' => true,
                            'trace_templates' => false,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'test.html.twig' => 'Hello {{ name }}!',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $result = $twig->render('test.html.twig', ['name' => 'World']);

        self::assertSame('Hello World!', $result);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        foreach ($spans as $span) {
            $attributes = $span->attributes();
            self::assertNotSame('template', $attributes['twig.type'] ?? '', 'Template span should not be traced when trace_templates is false');
        }
    }

    public function test_excluded_template_cascades_to_children() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'twig' => [
                            'enabled' => true,
                            'trace_blocks' => true,
                            'exclude_templates' => ['excluded.html.twig'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'child.html.twig' => 'Child content',
            'excluded.html.twig' => '{% block content %}Block content{% endblock %}{% include "child.html.twig" %}',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $twig->render('excluded.html.twig');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $templateNames = [];
        $blockNames = [];

        foreach ($spans as $span) {
            $attributes = $span->attributes();
            $type = $attributes['twig.type'] ?? '';

            if ($type === 'template') {
                $templateNames[] = $attributes['twig.template'];
            } elseif ($type === 'block') {
                $blockNames[] = $attributes['twig.name'];
            }
        }

        self::assertNotContains('excluded.html.twig', $templateNames, 'Excluded template should not be traced');
        self::assertNotContains('child.html.twig', $templateNames, 'Child template should not be traced when parent is excluded');
        self::assertNotContains('content', $blockNames, 'Block in excluded template should not be traced');
    }

    public function test_extension_not_registered_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'instrumentation' => [
                        'twig' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertFalse($container->has('flow.telemetry.twig.extension'));
    }

    public function test_extension_service_is_registered() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'instrumentation' => [
                        'twig' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.twig.extension'));
        self::assertInstanceOf(TracingTwigExtension::class, $container->get('flow.telemetry.twig.extension'));
    }

    public function test_traces_blocks_in_templates() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'twig' => [
                            'enabled' => true,
                            'trace_blocks' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'base.html.twig' => '{% block content %}Default content{% endblock %}',
            'child.html.twig' => '{% extends "base.html.twig" %}{% block content %}Child content{% endblock %}',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $result = $twig->render('child.html.twig');

        self::assertSame('Child content', $result);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertGreaterThanOrEqual(1, \count($spans));

        $blockSpanFound = false;

        foreach ($spans as $span) {
            $attributes = $span->attributes();

            if (($attributes['twig.type'] ?? '') === 'block') {
                $blockSpanFound = true;
                self::assertSame('content', $attributes['twig.name']);
            }
        }

        self::assertTrue($blockSpanFound, 'Expected block span was not found');
    }

    public function test_traces_macros_when_enabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'twig' => [
                            'enabled' => true,
                            'trace_macros' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'macros.html.twig' => '{% macro greet(name) %}Hello {{ name }}!{% endmacro %}',
            'test.html.twig' => '{% import "macros.html.twig" as macros %}{{ macros.greet("World") }}',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $result = $twig->render('test.html.twig');

        self::assertSame('Hello World!', $result);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $macroSpanFound = false;

        foreach ($spans as $span) {
            $attributes = $span->attributes();

            if (($attributes['twig.type'] ?? '') === 'macro') {
                $macroSpanFound = true;
                self::assertSame('greet', $attributes['twig.name']);
            }
        }

        self::assertTrue($macroSpanFound, 'Expected macro span was not found');
    }

    public function test_traces_template_rendering() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'twig' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingTwigExtension $extension */
        $extension = $container->get('flow.telemetry.twig.extension');

        $loader = new ArrayLoader([
            'test.html.twig' => 'Hello {{ name }}!',
        ]);

        $twig = new Environment($loader);
        $twig->addExtension($extension);

        $result = $twig->render('test.html.twig', ['name' => 'World']);

        self::assertSame('Hello World!', $result);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertGreaterThanOrEqual(1, \count($spans));

        $templateSpanFound = false;

        foreach ($spans as $span) {
            if ($span->name() === 'test.html.twig') {
                $templateSpanFound = true;
                $attributes = $span->attributes();
                self::assertSame('template', $attributes['twig.type']);
                self::assertSame('test.html.twig', $attributes['twig.template']);
            }
        }

        self::assertTrue($templateSpanFound, 'Expected template span was not found');
    }
}
