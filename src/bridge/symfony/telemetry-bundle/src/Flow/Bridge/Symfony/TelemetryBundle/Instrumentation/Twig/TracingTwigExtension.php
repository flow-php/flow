<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig;

use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\{Span, SpanKind, Tracer};
use Twig\Extension\AbstractExtension;
use Twig\Profiler\NodeVisitor\ProfilerNodeVisitor;
use Twig\Profiler\Profile;

final class TracingTwigExtension extends AbstractExtension
{
    /**
     * @var \SplObjectStorage<Profile, array{span: Span, tracer: Tracer}>
     */
    private \SplObjectStorage $activeSpans;

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
        $this->activeSpans = new \SplObjectStorage();
    }

    public function enter(Profile $profile) : void
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

        $tracer = $this->telemetry->tracer('flow.symfony.twig');

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

    #[\Override]
    public function getNodeVisitors() : array
    {
        return [new ProfilerNodeVisitor(self::class)];
    }

    public function leave(Profile $profile) : void
    {
        if ($profile->isTemplate() && $this->isTemplateExcluded($profile->getTemplate())) {
            $this->excludedDepth--;

            return;
        }

        if (!$this->activeSpans->contains($profile)) {
            return;
        }

        /** @var array{span: Span, tracer: Tracer} $spanData */
        $spanData = $this->activeSpans[$profile];
        $spanData['tracer']->complete($spanData['span']);

        $this->activeSpans->detach($profile);
    }

    private function getSpanName(Profile $profile) : string
    {
        if ($profile->isRoot()) {
            return $profile->getName();
        }

        if ($profile->isTemplate()) {
            return $profile->getTemplate();
        }

        return \sprintf(
            '%s::%s(%s)',
            $profile->getTemplate(),
            $profile->getType(),
            $profile->getName()
        );
    }

    private function isTemplateExcluded(string $template) : bool
    {
        foreach ($this->excludeTemplates as $pattern) {
            if ($this->matchesPattern($template, $pattern)) {
                return true;
            }
        }

        return false;
    }

    private function matchesPattern(string $template, string $pattern) : bool
    {
        if (\str_starts_with($pattern, '/') && \str_ends_with($pattern, '/')) {
            return (bool) \preg_match($pattern, $template);
        }

        return $template === $pattern;
    }

    private function shouldTrace(Profile $profile) : bool
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
