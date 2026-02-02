<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use function Flow\ETL\DSL\{config_builder, flow_context, int_entry, row, rows};
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Tests\FlowTestCase;

final class CachingProcessorTest extends FlowTestCase
{
    public function test_caches_batches_and_yields_them() : void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $processor = new CachingProcessor('test-cache');

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)));
            yield rows(row(int_entry('id', 2)));
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        self::assertCount(2, $result);
        self::assertTrue($cache->has('test-cache'));
    }

    public function test_handles_empty_input() : void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $processor = new CachingProcessor('test-cache');

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        self::assertCount(0, $result);
        self::assertTrue($cache->has('test-cache'));
    }

    public function test_passes_through_when_cache_exists() : void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $cache->set('test-cache', new CacheIndex('test-cache'));

        $processor = new CachingProcessor('test-cache');

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)));
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        self::assertCount(1, $result);
    }

    public function test_uses_config_id_when_no_id_provided() : void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->id('my-pipeline-id')->build());

        $processor = new CachingProcessor();

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)));
        })();

        iterator_to_array($processor->process($generator, $context));

        self::assertTrue($cache->has('my-pipeline-id'));
    }
}
