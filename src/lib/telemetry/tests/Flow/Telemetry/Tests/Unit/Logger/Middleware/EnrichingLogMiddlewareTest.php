<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Middleware;

use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\enriching_log_middleware;

final class EnrichingLogMiddlewareTest extends TestCase
{
    public function test_merges_default_attributes_into_the_record(): void
    {
        $middleware = enriching_log_middleware(['deployment.environment' => 'prod']);

        $enriched = $middleware->process(LogEntryMother::create('msg', Severity::INFO, ['order_id' => 7]));

        static::assertNotNull($enriched);
        static::assertSame('prod', $enriched->record->attributes->get('deployment.environment'));
        static::assertSame(7, $enriched->record->attributes->get('order_id'));
    }

    public function test_call_site_attributes_win_over_defaults(): void
    {
        $middleware = enriching_log_middleware(['deployment.environment' => 'prod']);

        $enriched = $middleware->process(LogEntryMother::create('msg', Severity::INFO, [
            'deployment.environment' => 'dev',
        ]));

        static::assertNotNull($enriched);
        static::assertSame('dev', $enriched->record->attributes->get('deployment.environment'));
    }

    public function test_preserves_entry_context_when_enriching(): void
    {
        $middleware = enriching_log_middleware(['k' => 'v']);
        $entry = LogEntryMother::create('msg', Severity::WARN);

        $enriched = $middleware->process($entry);

        static::assertNotNull($enriched);
        static::assertSame($entry->resource, $enriched->resource);
        static::assertSame($entry->scope, $enriched->scope);
        static::assertSame($entry->timestamp, $enriched->timestamp);
        static::assertSame(Severity::WARN, $enriched->record->severity);
        static::assertSame('msg', $enriched->record->body);
    }

    public function test_returns_entry_unchanged_when_no_attributes(): void
    {
        $middleware = enriching_log_middleware([]);
        $entry = LogEntryMother::create('msg', Severity::INFO);

        static::assertSame($entry, $middleware->process($entry));
    }
}
