<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig\TracingTwigExtension;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogProcessor, VoidMetricProcessor};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, TracerProvider};
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Twig\Profiler\NodeVisitor\ProfilerNodeVisitor;
use Twig\Profiler\Profile;

#[CoversClass(TracingTwigExtension::class)]
final class TracingTwigExtensionTest extends TestCase
{
    public function test_excluded_template_does_not_trace_nested_blocks() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            traceBlocks: true,
            excludeTemplates: ['@WebProfiler/layout.html.twig']
        );

        $templateProfile = new Profile('@WebProfiler/layout.html.twig', Profile::TEMPLATE, '@WebProfiler/layout.html.twig');
        $blockProfile = new Profile('@WebProfiler/layout.html.twig', Profile::BLOCK, 'content');

        $extension->enter($templateProfile);
        $extension->enter($blockProfile);
        $extension->leave($blockProfile);
        $extension->leave($templateProfile);

        self::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_get_node_visitors_returns_profiler_node_visitor() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry);

        $nodeVisitors = $extension->getNodeVisitors();

        self::assertCount(1, $nodeVisitors);
        self::assertInstanceOf(ProfilerNodeVisitor::class, $nodeVisitors[0]);
    }

    public function test_get_span_name_with_block_profile_returns_formatted_string() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceBlocks: true);

        $profile = new Profile('templates/page.html.twig', Profile::BLOCK, 'content');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('templates/page.html.twig::block(content)', $spans[0]->name());
    }

    public function test_get_span_name_with_macro_profile_returns_formatted_string() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceMacros: true);

        $profile = new Profile('macros/buttons.html.twig', Profile::MACRO, 'renderButton');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('macros/buttons.html.twig::macro(renderButton)', $spans[0]->name());
    }

    public function test_get_span_name_with_root_profile_returns_profile_name() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry);

        $profile = new Profile('main');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('main', $spans[0]->name());
    }

    public function test_get_span_name_with_template_profile_returns_template_path() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true);

        $profile = new Profile('templates/base.html.twig', Profile::TEMPLATE, 'templates/base.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('templates/base.html.twig', $spans[0]->name());
    }

    public function test_is_template_excluded_exact_match() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            excludeTemplates: ['templates/admin/secret.html.twig']
        );

        $profile = new Profile('templates/admin/secret.html.twig', Profile::TEMPLATE, 'templates/admin/secret.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        self::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_is_template_excluded_no_match_returns_false() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            excludeTemplates: ['/^@WebProfiler/', 'templates/secret.html.twig']
        );

        $profile = new Profile('templates/public.html.twig', Profile::TEMPLATE, 'templates/public.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('templates/public.html.twig', $spans[0]->name());
    }

    public function test_is_template_excluded_regex_match() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            excludeTemplates: ['/^@WebProfiler/']
        );

        $profile = new Profile('@WebProfiler/layout.html.twig', Profile::TEMPLATE, '@WebProfiler/layout.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        self::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_respects_block_flag() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceBlocks: false);

        $profile = new Profile('templates/page.html.twig', Profile::BLOCK, 'content');
        $extension->enter($profile);
        $extension->leave($profile);

        self::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_respects_macro_flag() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceMacros: false);

        $profile = new Profile('macros/buttons.html.twig', Profile::MACRO, 'renderButton');
        $extension->enter($profile);
        $extension->leave($profile);

        self::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_respects_template_flag() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: false);

        $profile = new Profile('templates/base.html.twig', Profile::TEMPLATE, 'templates/base.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        self::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_span_has_correct_attributes_for_block() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceBlocks: true);

        $profile = new Profile('templates/layout.html.twig', Profile::BLOCK, 'header');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $attributes = $spans[0]->attributes();
        self::assertSame(Profile::BLOCK, $attributes['twig.type']);
        self::assertSame('templates/layout.html.twig', $attributes['twig.template']);
        self::assertSame('header', $attributes['twig.name']);
    }

    public function test_span_has_correct_attributes_for_template() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true);

        $profile = new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $attributes = $spans[0]->attributes();
        self::assertSame(Profile::TEMPLATE, $attributes['twig.type']);
        self::assertSame('templates/home.html.twig', $attributes['twig.template']);
        self::assertArrayNotHasKey('twig.name', $attributes);
    }

    public function test_span_kind_is_internal() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $extension = new TracingTwigExtension($telemetry);

        $profile = new Profile('main');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(SpanKind::INTERNAL, $spans[0]->kind());
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor) : Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }
}
