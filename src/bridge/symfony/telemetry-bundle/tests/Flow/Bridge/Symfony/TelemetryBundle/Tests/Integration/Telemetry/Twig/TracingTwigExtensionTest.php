<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Telemetry\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Telemetry\Twig\TracingTwigExtension;
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
                    'service' => ['name' => 'test-app'],
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

    public function test_extension_not_registered_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
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
                    'service' => ['name' => 'test-app'],
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
                    'service' => ['name' => 'test-app'],
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

    public function test_traces_template_rendering() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
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
