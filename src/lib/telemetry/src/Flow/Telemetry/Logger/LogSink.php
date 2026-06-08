<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

/**
 * A terminal {@see LogProcessor} - the leaf/final step of a
 * {@see Processor\PipelineLogProcessor}, which exports (or otherwise consumes)
 * the entries that survive the middleware chain.
 *
 * It is a marker over {@see LogProcessor} that adds no methods; its only purpose
 * is to let a pipeline's sink slot be typed so a {@see LogMiddleware} - or a
 * pipeline itself - cannot accidentally be wired there. The batching, pass-through,
 * memory, void and composite processors are sinks.
 */
interface LogSink extends LogProcessor {}
