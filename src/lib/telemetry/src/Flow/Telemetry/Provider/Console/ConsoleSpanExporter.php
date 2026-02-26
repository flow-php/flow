<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

use Flow\Telemetry\Resource as TelemetryResource;
use Flow\Telemetry\Tracer\{Span, SpanExporter, SpanStatusCode};
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * Console exporter for spans with ASCII table visualization.
 *
 * Outputs spans to the console in a human-readable format with optional ANSI colors.
 * Table width is calculated dynamically based on content.
 */
final readonly class ConsoleSpanExporter implements SpanExporter
{
    private const int MIN_WIDTH = 40;

    private ConsoleOutput $output;

    /**
     * @param bool $colors Whether to use ANSI colors
     * @param null|resource $outputStream Output stream (default: STDOUT)
     */
    public function __construct(
        bool $colors = true,
        mixed $outputStream = null,
        private ConsoleSpanOptions $options = new ConsoleSpanOptions(),
    ) {
        /** @var null|resource $outputStream */
        $this->output = new ConsoleOutput($colors, $outputStream);
    }

    /**
     * @param array<Span> $spans
     */
    public function export(array $spans) : bool
    {
        foreach ($spans as $span) {
            $this->printSpan($span);
        }

        return true;
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }

    /**
     * @return array<string>
     */
    private function buildAttributeLines(Span $span) : array
    {
        $attributes = $span->attributes();

        if (\count($attributes) === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Attributes:');

        $maxKeyLength = 0;

        foreach (\array_keys($attributes) as $key) {
            $maxKeyLength = \max($maxKeyLength, \mb_strlen($key));
        }

        foreach ($attributes as $key => $value) {
            $keyStr = $this->output->pad($key, $maxKeyLength);
            $valueStr = $this->output->formatValue($value);
            $lines[] = '  ' . $this->output->cyan($keyStr) . ' = ' . $valueStr;
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildDroppedCountsLines(Span $span) : array
    {
        if (!$this->options->showDroppedCounts) {
            return [];
        }

        $droppedAttrs = $span->droppedAttributeCount();
        $droppedEvents = $span->droppedEventsCount();
        $droppedLinks = $span->droppedLinksCount();

        if ($droppedAttrs === 0 && $droppedEvents === 0 && $droppedLinks === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Dropped:');

        if ($droppedAttrs > 0) {
            $lines[] = '  ' . $this->output->yellow('Attributes: ' . $droppedAttrs);
        }

        if ($droppedEvents > 0) {
            $lines[] = '  ' . $this->output->yellow('Events: ' . $droppedEvents);
        }

        if ($droppedLinks > 0) {
            $lines[] = '  ' . $this->output->yellow('Links: ' . $droppedLinks);
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildEventLines(Span $span) : array
    {
        $events = $span->events();

        if (\count($events) === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Events (' . \count($events) . '):');

        foreach ($events as $event) {
            $timestamp = $this->formatTimestamp($event->timestamp());
            $attrs = $event->attributes();
            $attrStr = \count($attrs) > 0 ? ' ' . $this->output->dim($this->output->formatValue($attrs)) : '';
            $lines[] = '  ' . $this->output->gray($timestamp) . ' ' . $event->name() . $attrStr;
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildLinkLines(Span $span) : array
    {
        if (!$this->options->showLinks) {
            return [];
        }

        $links = $span->links();

        if (\count($links) === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Links (' . \count($links) . '):');

        foreach ($links as $link) {
            $traceId = $link->context->traceId->toHex();
            $spanId = $link->context->spanId->toHex();
            $lines[] = '  -> ' . $this->output->dim('trace:' . \mb_substr($traceId, 0, 12) . '... span:' . \mb_substr($spanId, 0, 12) . '...');
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildResourceLines(TelemetryResource $resource) : array
    {
        if ($resource->isEmpty()) {
            return [];
        }

        if (!$this->options->showResourceAttributes) {
            $parts = [];
            $serviceName = $resource->get('service.name');

            if (\is_string($serviceName)) {
                $parts[] = $serviceName;
            }

            $serviceVersion = $resource->get('service.version');

            if (\is_string($serviceVersion)) {
                $parts[] = 'v' . $serviceVersion;
            }

            if (\count($parts) === 0) {
                return [];
            }

            return ['Resource: ' . $this->output->dim(\implode(' ', $parts))];
        }

        $lines = [];
        $lines[] = $this->output->bold('Resource:');

        $attributes = $resource->all();
        $maxKeyLength = 0;

        foreach (\array_keys($attributes) as $key) {
            $maxKeyLength = \max($maxKeyLength, \mb_strlen($key));
        }

        foreach ($attributes as $key => $value) {
            $keyStr = $this->output->pad($key, $maxKeyLength);
            $valueStr = $this->output->formatValue($value);
            $lines[] = '  ' . $this->output->cyan($keyStr) . ' = ' . $valueStr;
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildScopeLines(Span $span) : array
    {
        if (!$this->options->showInstrumentationScope) {
            return [];
        }

        $scope = $span->scope();

        return ['Scope: ' . $this->output->dim($scope->name . ' v' . $scope->version)];
    }

    /**
     * @return array<string>
     */
    private function buildSpanLines(Span $span) : array
    {
        $context = $span->context();
        $lines = [];

        $lines[] = 'SPAN: ' . $span->name();

        $traceInfo = 'Trace: ' . $this->output->dim($context->traceId->toHex());
        $traceInfo .= '  Span: ' . $this->output->dim($context->spanId->toHex());

        if ($context->parentSpanId !== null) {
            $traceInfo .= '  Parent: ' . $this->output->dim($context->parentSpanId->toHex());
        }

        $lines[] = $traceInfo;

        $kindStr = $this->output->cyan(\strtoupper($span->kind()->value));
        $statusStr = $this->formatStatusIcon($span);
        $durationStr = $this->output->formatDuration($span->duration());

        $details = $this->output->pad('Kind: ' . $kindStr, 22);
        $details .= 'Status: ' . $statusStr . '  ';
        $details .= 'Duration: ' . $durationStr;
        $lines[] = $details;

        $lines[] = 'Start: ' . $this->output->formatTimestamp($span->startTime());

        return $lines;
    }

    /**
     * @param array<string> $lines
     */
    private function calculateWidth(array $lines) : int
    {
        $maxLength = 0;

        foreach ($lines as $line) {
            $visibleLength = \mb_strlen($this->output->stripColors($line));
            $maxLength = \max($maxLength, $visibleLength);
        }

        return \max(self::MIN_WIDTH, $maxLength + 4);
    }

    private function formatStatusIcon(Span $span) : string
    {
        $status = $span->status();

        if ($status === null) {
            return $this->output->yellow('UNSET');
        }

        $statusText = match ($status->code) {
            SpanStatusCode::OK => $this->output->green('OK'),
            SpanStatusCode::ERROR => $this->output->red('ERROR'),
            SpanStatusCode::UNSET => $this->output->yellow('UNSET'),
        };

        if ($this->options->showStatusDescription && $status->code === SpanStatusCode::ERROR && $status->description !== null) {
            $statusText .= ' ' . $this->output->dim('(' . $status->description . ')');
        }

        return $statusText;
    }

    private function formatTimestamp(\DateTimeImmutable $timestamp) : string
    {
        return $timestamp->format('H:i:s.u');
    }

    private function printSpan(Span $span) : void
    {
        $headerLines = $this->buildSpanLines($span);
        $resourceLines = $this->buildResourceLines($span->resource());
        $scopeLines = $this->buildScopeLines($span);
        $attributeLines = $this->buildAttributeLines($span);
        $eventLines = $this->buildEventLines($span);
        $linkLines = $this->buildLinkLines($span);
        $droppedLines = $this->buildDroppedCountsLines($span);

        $allLines = \array_merge($headerLines, $resourceLines, $scopeLines, $attributeLines, $eventLines, $linkLines, $droppedLines);
        $width = $this->calculateWidth($allLines);

        $buffer = $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->output->bold($headerLines[0]), $width) . PHP_EOL;
        $buffer .= $this->output->border($width) . PHP_EOL;

        for ($i = 1; $i < \count($headerLines); $i++) {
            $buffer .= $this->output->row($headerLines[$i], $width) . PHP_EOL;
        }

        if (\count($resourceLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($resourceLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (\count($scopeLines) > 0) {
            foreach ($scopeLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (\count($attributeLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($attributeLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (\count($eventLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($eventLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (\count($linkLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($linkLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (\count($droppedLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($droppedLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        $buffer .= $this->output->border($width);

        $this->output->write($buffer);
    }
}
