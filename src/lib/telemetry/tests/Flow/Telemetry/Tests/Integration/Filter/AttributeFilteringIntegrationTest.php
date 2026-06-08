<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Filter;

use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\TempDir;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_log_middleware;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\memory_exporter;
use function Flow\Telemetry\DSL\memory_log_processor;
use function Flow\Telemetry\DSL\pipeline_log_processor;

final class AttributeFilteringIntegrationTest extends TestCase
{
    public function test_filters_logs_through_the_processor_chain_into_memory(): void
    {
        $tmp = TempDir::create();

        try {
            $memory = memory_log_processor(memory_exporter());
            $processor = pipeline_log_processor([attribute_filtering_log_middleware(attribute_filter(
                attribute_rule('http.user_agent', MatchMode::CONTAINS, 'bot', false),
                cacheDir: $tmp->path(),
            ))], $memory);

            $processor->process(LogEntryMother::create('human request', Severity::INFO, [
                'http.user_agent' => 'Mozilla/5.0',
            ]));
            $processor->process(LogEntryMother::create('bot request', Severity::INFO, [
                'http.user_agent' => 'GoogleBot/2.1',
            ]));
            $processor->flush();

            static::assertSame(1, $memory->countLogs());
            static::assertSame('human request', $memory->entries()[0]->record->body);
            static::assertCount(1, $tmp->files());
        } finally {
            $tmp->remove();
        }
    }
}
