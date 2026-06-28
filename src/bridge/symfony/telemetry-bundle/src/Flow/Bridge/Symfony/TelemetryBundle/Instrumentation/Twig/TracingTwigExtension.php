<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig;

use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Override;
use SplObjectStorage;
use Twig\Extension\AbstractExtension;
use Twig\Profiler\NodeVisitor\ProfilerNodeVisitor;
use Twig\Profiler\Profile;

use function is_array;
use function preg_match;
use function sprintf;

final class TracingTwigExtension extends AbstractExtension
{
    private SplObjectStorage $activeSpans;

    private int $excludedDepth = 0;

    /**
     * @param array<string> $excludeTemplates
     */
    public function __construct(
        private readonly Telemetry $telemetry,
        private readonly bool $traceTemplates = true,
        private readonly bool $traceBlocks = false,
        private readonly bool $traceMacros = false,
        private readonly array $excludeTemplates = [],
    ) {
        $this->activeSpans = new SplObjectStorage();
    }

    public function enter(Profile $profile): void
    {
        if ($profile->isTemplate() && $this->isTemplateExcluded($profile->getTemplate())) {
            $this->excludedDepth++;

            return;
        }

        if ($this->excludedDepth > 0) {
            return;
        }

        if (!$this->shouldTrace($profile)) {
            return;
        }

        $tracer = $this->telemetry->tracer('flow.symfony.twig', PackageVersion::get('twig/twig'));

        $spanName = $this->getSpanName($profile);
        $attributes = [
            'twig.type' => $profile->getType(),
            'twig.template' => $profile->getTemplate(),
        ];

        if (!$profile->isRoot() && !$profile->isTemplate()) {
            $attributes['twig.name'] = $profile->getName();
        }

        $span = $tracer->span($spanName, SpanKind::INTERNAL, $attributes);

        $this->activeSpans[$profile] = ['span' => $span, 'tracer' => $tracer];
    }

    #[Override]
    public function getNodeVisitors(): array
    {
        return [new ProfilerNodeVisitor(self::class)];
    }

    public function leave(Profile $profile): void
    {
        if ($profile->isTemplate() && $this->isTemplateExcluded($profile->getTemplate())) {
            $this->excludedDepth--;

            return;
        }

        if (!isset($this->activeSpans[$profile])) {
            return;
        }

        // @mago-expect analysis:impossible-assignment
        $spanData = $this->activeSpans[$profile];

        // @mago-expect analysis:no-value(2),redundant-type-comparison(2),redundant-logical-operation(2)
        if (is_array($spanData) && $spanData['tracer'] instanceof Tracer && $spanData['span'] instanceof Span) {
            // OTEL spec: instrumentation leaves the status Unset on success.
            $spanData['tracer']->complete($spanData['span']);
        }

        unset($this->activeSpans[$profile]);
    }

    public function reset(): void
    {
        foreach ($this->activeSpans as $profile) {
            // @mago-expect analysis:mixed-assignment
            $spanData = $this->activeSpans[$profile];

            if (is_array($spanData) && $spanData['tracer'] instanceof Tracer && $spanData['span'] instanceof Span) {
                $spanData['span']->setAttribute('error.type', 'incomplete_render');
                $spanData['span']->setStatus(SpanStatus::error('Twig rendering did not complete'));
                $spanData['tracer']->complete($spanData['span']);
            }
        }

        $this->activeSpans = new SplObjectStorage();
        $this->excludedDepth = 0;
    }

    private function getSpanName(Profile $profile): string
    {
        if ($profile->isRoot()) {
            return $profile->getName();
        }

        if ($profile->isTemplate()) {
            return $profile->getTemplate();
        }

        return sprintf('%s::%s(%s)', $profile->getTemplate(), $profile->getType(), $profile->getName());
    }

    private function isTemplateExcluded(string $template): bool
    {
        foreach ($this->excludeTemplates as $pattern) {
            if ($this->matchesPattern($template, $pattern)) {
                return true;
            }
        }

        return false;
    }

    private function matchesPattern(string $template, string $pattern): bool
    {
        $result = @preg_match($pattern, $template);

        if ($result !== false) {
            return (bool) $result;
        }

        return $template === $pattern;
    }

    private function shouldTrace(Profile $profile): bool
    {
        if ($profile->isRoot()) {
            return true;
        }

        if ($profile->isTemplate()) {
            return $this->traceTemplates;
        }

        $type = $profile->getType();

        if ($type === Profile::BLOCK) {
            return $this->traceBlocks;
        }

        if ($type === Profile::MACRO) {
            return $this->traceMacros;
        }

        return true;
    }
}
