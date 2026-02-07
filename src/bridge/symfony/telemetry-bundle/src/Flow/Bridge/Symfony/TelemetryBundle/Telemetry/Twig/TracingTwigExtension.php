<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Telemetry\Twig;

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

    public function __construct(
        private readonly Telemetry $telemetry,
        private readonly bool $traceTemplates = true,
        private readonly bool $traceBlocks = false,
        private readonly bool $traceMacros = false,
    ) {
        $this->activeSpans = new \SplObjectStorage();
    }

    public function enter(Profile $profile) : void
    {
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
