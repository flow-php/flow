<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CachingProcessorTest extends FlowTestCase
{
    public function test_a_stop_still_completes_the_cache(): void
    {
        $cache = new InMemoryCache();
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $processed = (new CachingProcessor('stop-cache', $cache))->process(
            $upstream->extract(flow_context()),
            flow_context(),
        );

        static::assertTrue($processed->valid());

        $processed->send(Signal::STOP);

        static::assertFalse($processed->valid());
        static::assertSame(5, $upstream->batchesYielded);

        $indexRows = $cache->get('stop-cache');

        static::assertInstanceOf(Rows::class, $indexRows);
        static::assertCount(5, CacheIndex::fromRows('stop-cache', $indexRows)->values());
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));

        static::assertEquals($input, (new CachingProcessor('bind-cache'))->bind($input)->output);
    }

    public function test_caches_batches_and_yields_them(): void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $processor = new CachingProcessor('test-cache');

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]));
            yield rows(schema(int_schema('id')), row(['id' => 2]));
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        static::assertCount(2, $result);
        static::assertTrue($cache->has('test-cache'));

        $indexRows = $cache->get('test-cache');

        static::assertInstanceOf(Rows::class, $indexRows);

        $index = CacheIndex::fromRows('test-cache', $indexRows);

        static::assertCount(2, $index->values());

        foreach ($index->values() as $cacheKey) {
            static::assertTrue($cache->has($cacheKey));
        }
    }

    public function test_handles_empty_input(): void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $processor = new CachingProcessor('test-cache');

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        static::assertCount(0, $result);
        static::assertTrue($cache->has('test-cache'));
    }

    public function test_passes_through_when_cache_exists(): void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $cache->set('test-cache', (new CacheIndex('test-cache'))->toRows());

        $processor = new CachingProcessor('test-cache');

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]));
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        static::assertCount(1, $result);
    }

    public function test_uses_config_id_when_no_id_provided(): void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->id('my-pipeline-id')->build());

        $processor = new CachingProcessor();

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]));
        })();

        iterator_to_array($processor->process($generator, $context));

        static::assertTrue($cache->has('my-pipeline-id'));
    }
}
