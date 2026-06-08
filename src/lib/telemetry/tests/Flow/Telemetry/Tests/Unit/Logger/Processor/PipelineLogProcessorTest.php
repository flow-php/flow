<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\LogProcessorSpy;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\enriching_log_middleware;
use function Flow\Telemetry\DSL\pipeline_log_processor;
use function Flow\Telemetry\DSL\severity_filtering_log_middleware;

final class PipelineLogProcessorTest extends TestCase
{
    public function test_forwards_entry_to_the_sink_when_no_middleware(): void
    {
        $sink = new LogProcessorSpy();
        $pipeline = pipeline_log_processor([], $sink);

        $pipeline->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(1, $sink->processedCount());
    }

    public function test_runs_middleware_in_order_and_enrichment_reaches_the_sink(): void
    {
        $sink = new LogProcessorSpy();
        $pipeline = pipeline_log_processor([
            enriching_log_middleware(['first' => 'a']),
            enriching_log_middleware(['second' => 'b']),
        ], $sink);

        $pipeline->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(1, $sink->processedCount());
        // both enrichers' attributes accumulate on the record passed to the sink
        static::assertSame('a', $sink->processed()[0]->record->attributes->get('first'));
        static::assertSame('b', $sink->processed()[0]->record->attributes->get('second'));
    }

    public function test_earlier_enrichment_wins_over_a_later_default_for_the_same_key(): void
    {
        $sink = new LogProcessorSpy();
        // each enricher treats already-present attributes as call-site values, which win;
        // so the first enricher's value for a shared key survives the second.
        $pipeline = pipeline_log_processor([
            enriching_log_middleware(['stage' => 'one']),
            enriching_log_middleware(['stage' => 'two']),
        ], $sink);

        $pipeline->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame('one', $sink->processed()[0]->record->attributes->get('stage'));
    }

    public function test_dropping_middleware_short_circuits_the_sink(): void
    {
        $sink = new LogProcessorSpy();
        $pipeline = pipeline_log_processor([severity_filtering_log_middleware(Severity::WARN)], $sink);

        $pipeline->process(LogEntryMother::create('info', Severity::INFO));

        static::assertSame(0, $sink->processedCount());
    }

    public function test_dropping_middleware_skips_later_middleware(): void
    {
        $sink = new LogProcessorSpy();
        // severity drop happens before the enricher; if the enricher still ran and
        // forwarded, the sink would receive an entry. It must not.
        $pipeline = pipeline_log_processor([
            severity_filtering_log_middleware(Severity::WARN),
            enriching_log_middleware(['stage' => 'unreached']),
        ], $sink);

        $pipeline->process(LogEntryMother::create('info', Severity::INFO));

        static::assertSame(0, $sink->processedCount());
    }

    public function test_flush_and_shutdown_delegate_to_the_sink(): void
    {
        $sink = new LogProcessorSpy();
        $pipeline = pipeline_log_processor([severity_filtering_log_middleware()], $sink);

        static::assertTrue($pipeline->flush());
        $pipeline->shutdown();

        static::assertSame(1, $sink->flushCount());
        static::assertSame(1, $sink->shutdownCount());
    }
}
