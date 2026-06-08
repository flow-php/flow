<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Middleware;

use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\TempDir;
use PHPUnit\Framework\TestCase;

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
}
