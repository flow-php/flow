<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig\TracingTwigExtension;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Twig\Profiler\NodeVisitor\ProfilerNodeVisitor;
use Twig\Profiler\Profile;

#[CoversClass(TracingTwigExtension::class)]
final class TracingTwigExtensionTest extends TestCase
{
    public function test_excluded_template_does_not_trace_nested_blocks(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            traceBlocks: true,
            excludeTemplates: ['@WebProfiler/layout.html.twig'],
        );

        $templateProfile = new Profile(
            '@WebProfiler/layout.html.twig',
            Profile::TEMPLATE,
            '@WebProfiler/layout.html.twig',
        );
        $blockProfile = new Profile('@WebProfiler/layout.html.twig', Profile::BLOCK, 'content');

        $extension->enter($templateProfile);
        $extension->enter($blockProfile);
        $extension->leave($blockProfile);
        $extension->leave($templateProfile);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_get_node_visitors_returns_profiler_node_visitor(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry);

        $nodeVisitors = $extension->getNodeVisitors();

        static::assertCount(1, $nodeVisitors);
        static::assertInstanceOf(ProfilerNodeVisitor::class, $nodeVisitors[0]);
    }

    public function test_get_span_name_with_block_profile_returns_formatted_string(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceBlocks: true);

        $profile = new Profile('templates/page.html.twig', Profile::BLOCK, 'content');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('templates/page.html.twig::block(content)', $spans[0]->name());
    }

    public function test_get_span_name_with_macro_profile_returns_formatted_string(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceMacros: true);

        $profile = new Profile('macros/buttons.html.twig', Profile::MACRO, 'renderButton');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('macros/buttons.html.twig::macro(renderButton)', $spans[0]->name());
    }

    public function test_get_span_name_with_root_profile_returns_profile_name(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry);

        $profile = new Profile('main');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('main', $spans[0]->name());
    }

    public function test_get_span_name_with_template_profile_returns_template_path(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true);

        $profile = new Profile('templates/base.html.twig', Profile::TEMPLATE, 'templates/base.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('templates/base.html.twig', $spans[0]->name());
    }

    public function test_is_template_excluded_exact_match(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            excludeTemplates: ['templates/admin/secret.html.twig'],
        );

        $profile = new Profile(
            'templates/admin/secret.html.twig',
            Profile::TEMPLATE,
            'templates/admin/secret.html.twig',
        );
        $extension->enter($profile);
        $extension->leave($profile);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_is_template_excluded_no_match_returns_false(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            excludeTemplates: ['/^@WebProfiler/', 'templates/secret.html.twig'],
        );

        $profile = new Profile('templates/public.html.twig', Profile::TEMPLATE, 'templates/public.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('templates/public.html.twig', $spans[0]->name());
    }

    public function test_is_template_excluded_regex_match(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, excludeTemplates: ['/^@WebProfiler/']);

        $profile = new Profile('@WebProfiler/layout.html.twig', Profile::TEMPLATE, '@WebProfiler/layout.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_respects_block_flag(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceBlocks: false);

        $profile = new Profile('templates/page.html.twig', Profile::BLOCK, 'content');
        $extension->enter($profile);
        $extension->leave($profile);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_respects_macro_flag(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceMacros: false);

        $profile = new Profile('macros/buttons.html.twig', Profile::MACRO, 'renderButton');
        $extension->enter($profile);
        $extension->leave($profile);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_should_trace_respects_template_flag(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: false);

        $profile = new Profile('templates/base.html.twig', Profile::TEMPLATE, 'templates/base.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_span_has_correct_attributes_for_block(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true, traceBlocks: true);

        $profile = new Profile('templates/layout.html.twig', Profile::BLOCK, 'header');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $attributes = $spans[0]->attributes();
        static::assertSame(Profile::BLOCK, $attributes['twig.type']);
        static::assertSame('templates/layout.html.twig', $attributes['twig.template']);
        static::assertSame('header', $attributes['twig.name']);
    }

    public function test_span_has_correct_attributes_for_template(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true);

        $profile = new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $attributes = $spans[0]->attributes();
        static::assertSame(Profile::TEMPLATE, $attributes['twig.type']);
        static::assertSame('templates/home.html.twig', $attributes['twig.template']);
        static::assertArrayNotHasKey('twig.name', $attributes);
    }

    public function test_span_has_ok_status_after_leave(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true);

        $profile = new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_span_kind_is_internal(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry);

        $profile = new Profile('main');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(SpanKind::INTERNAL, $spans[0]->kind());
    }

    public function test_reset_completes_orphaned_span_as_error(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true);

        $profile = new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig');
        $extension->enter($profile);

        static::assertCount(0, $spanProcessor->endedSpans());

        $extension->reset();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Twig rendering did not complete', $status->description);
    }

    public function test_reset_clears_excluded_depth_so_tracing_resumes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension(
            $telemetry,
            traceTemplates: true,
            excludeTemplates: ['@WebProfiler/layout.html.twig'],
        );

        $excluded = new Profile('@WebProfiler/layout.html.twig', Profile::TEMPLATE, '@WebProfiler/layout.html.twig');
        $extension->enter($excluded);

        $extension->reset();

        $profile = new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('templates/home.html.twig', $spans[0]->name());
    }

    public function test_reset_without_active_spans_is_noop(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $extension = new TracingTwigExtension($telemetry, traceTemplates: true);

        $profile = new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig');
        $extension->enter($profile);
        $extension->leave($profile);

        $extension->reset();
        $extension->reset();

        static::assertCount(1, $spanProcessor->endedSpans());
    }
}
