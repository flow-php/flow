<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogMiddleware;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Logger\LogSink;

use function array_values;

/**
 * Runs a log entry through an ordered chain of {@see LogMiddleware} and forwards
 * the survivors to a single {@see LogSink}.
 *
 * Each middleware may enrich the entry (returning a new one) or drop it (returning
 * null); a drop short-circuits the chain so later middleware and the sink never see
 * it. The pipeline owns no buffer of its own - {@see self::flush()} and
 * {@see self::shutdown()} delegate to the sink, since middleware are stateless.
 *
 * This models the OpenTelemetry LogRecordProcessor pipeline: processors registered
 * in order, each able to modify the record, with mutations visible to the next.
 */
final readonly class PipelineLogProcessor implements LogProcessor
{
    /**
     * @var list<LogMiddleware>
     */
    private array $middleware;

    /**
     * @param list<LogMiddleware> $middleware run in order; the first to return null drops the entry
     */
    public function __construct(
        array $middleware,
        private LogSink $sink,
    ) {
        $this->middleware = array_values($middleware);
    }

    public function flush(): bool
    {
        return $this->sink->flush();
    }

    public function process(LogEntry $entry): void
    {
        foreach ($this->middleware as $middleware) {
            $processed = $middleware->process($entry);

            if ($processed === null) {
                return;
            }

            $entry = $processed;
        }

        $this->sink->process($entry);
    }

    public function shutdown(): void
    {
        $this->sink->shutdown();
    }
}
