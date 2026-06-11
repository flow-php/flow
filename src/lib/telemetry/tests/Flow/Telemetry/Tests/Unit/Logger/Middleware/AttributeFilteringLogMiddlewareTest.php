<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Middleware;

use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Logger\Middleware\AttributeFilteringLogMiddleware;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\TempDir;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\all;
use function Flow\Telemetry\DSL\any;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_log_middleware;
use function Flow\Telemetry\DSL\attribute_rule;

final class AttributeFilteringLogMiddlewareTest extends TestCase
{
    public function test_drops_entry_matching_signal_attribute(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule('env', MatchMode::EQUAL, 'prod'),
                cacheDir: $tmp->path(),
            ));

            static::assertNull($middleware->process(LogEntryMother::create('dropped', Severity::INFO, [
                'env' => 'prod',
            ])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_forwards_entry_not_matching_filter(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule('env', MatchMode::EQUAL, 'prod'),
                cacheDir: $tmp->path(),
            ));

            $entry = LogEntryMother::create('kept', Severity::INFO, ['env' => 'dev']);

            static::assertSame($entry, $middleware->process($entry));
        } finally {
            $tmp->remove();
        }
    }

    public function test_filters_on_resource_attributes(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule('service.name', MatchMode::EQUAL, 'test-service'),
                sources: [AttributeSource::RESOURCE],
                cacheDir: $tmp->path(),
            ));

            static::assertNull($middleware->process(LogEntryMother::create('dropped', Severity::INFO, [
                'env' => 'dev',
            ])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_filters_on_scope_attributes_keeps_entry_without_matching_scope(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule('scope.flag', MatchMode::EQUAL, 'on'),
                sources: [AttributeSource::SCOPE],
                cacheDir: $tmp->path(),
            ));

            $entry = LogEntryMother::create('kept', Severity::INFO, ['scope.flag' => 'on']);

            static::assertSame($entry, $middleware->process($entry));
        } finally {
            $tmp->remove();
        }
    }

    public function test_filters_across_multiple_sources_with_or(): void
    {
        $tmp = TempDir::create();

        try {
            // service.name lives on the resource, not the signal; with both sources
            // listed the resource match alone drops the entry.
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule('service.name', MatchMode::EQUAL, 'test-service'),
                sources: [AttributeSource::SIGNAL, AttributeSource::RESOURCE],
                cacheDir: $tmp->path(),
            ));

            static::assertNull($middleware->process(LogEntryMother::create('dropped', Severity::INFO, [
                'env' => 'dev',
            ])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_keeps_entry_at_or_above_severity_threshold(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule(
                    AttributeFilteringLogMiddleware::SEVERITY_KEY,
                    MatchMode::GREATER_THAN_EQUAL,
                    Severity::ERROR->value,
                ),
                exclude: false,
                cacheDir: $tmp->path(),
            ));

            $entry = LogEntryMother::create('boom', Severity::ERROR);

            static::assertSame($entry, $middleware->process($entry));
        } finally {
            $tmp->remove();
        }
    }

    public function test_drops_entry_below_severity_threshold(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule(
                    AttributeFilteringLogMiddleware::SEVERITY_KEY,
                    MatchMode::GREATER_THAN_EQUAL,
                    Severity::ERROR->value,
                ),
                exclude: false,
                cacheDir: $tmp->path(),
            ));

            static::assertNull($middleware->process(LogEntryMother::create('chatter', Severity::INFO)));
        } finally {
            $tmp->remove();
        }
    }

    public function test_per_channel_severity_thresholds(): void
    {
        $tmp = TempDir::create();

        try {
            // Keep ERROR+ from the payments channel, but DEBUG+ from the importer channel.
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                any(
                    all(
                        attribute_rule('log.channel', MatchMode::EQUAL, 'payments'),
                        attribute_rule(
                            AttributeFilteringLogMiddleware::SEVERITY_KEY,
                            MatchMode::GREATER_THAN_EQUAL,
                            Severity::ERROR->value,
                        ),
                    ),
                    all(
                        attribute_rule('log.channel', MatchMode::EQUAL, 'importer'),
                        attribute_rule(
                            AttributeFilteringLogMiddleware::SEVERITY_KEY,
                            MatchMode::GREATER_THAN_EQUAL,
                            Severity::DEBUG->value,
                        ),
                    ),
                ),
                exclude: false,
                cacheDir: $tmp->path(),
            ));

            $paymentsError = LogEntryMother::create('payment failed', Severity::ERROR, ['log.channel' => 'payments']);
            $importerDebug = LogEntryMother::create('row parsed', Severity::DEBUG, ['log.channel' => 'importer']);

            static::assertSame($paymentsError, $middleware->process($paymentsError));
            static::assertSame($importerDebug, $middleware->process($importerDebug));
            static::assertNull($middleware->process(LogEntryMother::create('payment ok', Severity::INFO, [
                'log.channel' => 'payments',
            ])));
            static::assertNull($middleware->process(LogEntryMother::create('importer trace', Severity::TRACE, [
                'log.channel' => 'importer',
            ])));
            static::assertNull($middleware->process(LogEntryMother::create('other error', Severity::ERROR, [
                'log.channel' => 'web',
            ])));
        } finally {
            $tmp->remove();
        }
    }

    public function test_matches_on_severity_name(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule(AttributeFilteringLogMiddleware::SEVERITY_NAME_KEY, MatchMode::EQUAL, 'ERROR'),
                cacheDir: $tmp->path(),
            ));

            static::assertNull($middleware->process(LogEntryMother::create('boom', Severity::ERROR)));

            $kept = LogEntryMother::create('chatter', Severity::INFO);
            static::assertSame($kept, $middleware->process($kept));
        } finally {
            $tmp->remove();
        }
    }

    public function test_does_not_leak_synthetic_severity_keys_onto_kept_entry(): void
    {
        $tmp = TempDir::create();

        try {
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule('env', MatchMode::EQUAL, 'prod'),
                cacheDir: $tmp->path(),
            ));

            $entry = LogEntryMother::create('kept', Severity::ERROR, ['env' => 'dev']);
            $processed = $middleware->process($entry);

            static::assertSame($entry, $processed);
            static::assertFalse($processed->record->attributes->has(AttributeFilteringLogMiddleware::SEVERITY_KEY));
            static::assertFalse($processed->record->attributes->has(AttributeFilteringLogMiddleware::SEVERITY_NAME_KEY));
        } finally {
            $tmp->remove();
        }
    }

    public function test_synthetic_severity_shadows_user_attribute_of_same_name(): void
    {
        $tmp = TempDir::create();

        try {
            // A user attribute named log.severity must not fool a severity threshold:
            // the record's real severity (INFO) is below ERROR, so the entry is dropped
            // despite the user claiming ERROR's number.
            $middleware = attribute_filtering_log_middleware(attribute_filter(
                attribute_rule(
                    AttributeFilteringLogMiddleware::SEVERITY_KEY,
                    MatchMode::GREATER_THAN_EQUAL,
                    Severity::ERROR->value,
                ),
                exclude: false,
                cacheDir: $tmp->path(),
            ));

            static::assertNull($middleware->process(LogEntryMother::create('spoofed', Severity::INFO, [
                AttributeFilteringLogMiddleware::SEVERITY_KEY => Severity::ERROR->value,
            ])));
        } finally {
            $tmp->remove();
        }
    }
}
