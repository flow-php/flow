<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit\Double\{SpyClient, SpyMarshaller, TestableCacheAdapter};
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Exception\InvalidArgumentException;

final class FlowPostgreSqlCacheAdapterTest extends TestCase
{
    public function test_clear_with_empty_namespace_truncates_table() : void
    {
        $client = new SpyClient();
        $adapter = new TestableCacheAdapter($client);

        self::assertTrue($adapter->exposedDoClear(''));
        self::assertCount(1, $client->executedQueries);
        self::assertSame('TRUNCATE public.cache_items', $client->executedQueries[0]['sql']);
        self::assertSame([], $client->executedQueries[0]['parameters']);
    }

    public function test_clear_with_namespace_uses_like_pattern() : void
    {
        $client = new SpyClient();
        $adapter = new TestableCacheAdapter($client);

        self::assertTrue($adapter->exposedDoClear('user:'));
        self::assertSame('DELETE FROM public.cache_items WHERE item_id LIKE $1', $client->executedQueries[0]['sql']);
        self::assertSame(['user:%'], $client->executedQueries[0]['parameters']);
    }

    public function test_custom_marshaller_is_used_when_supplied() : void
    {
        $client = new SpyClient();
        $marshaller = new SpyMarshaller();
        $adapter = new TestableCacheAdapter($client, marshaller: $marshaller);

        $adapter->exposedDoSave(['foo' => 'bar'], 0);

        self::assertSame(1, $marshaller->marshallCalls);
        self::assertArrayHasKey('foo', $marshaller->marshalled);
    }

    public function test_delete_uses_in_clause() : void
    {
        $client = new SpyClient();
        $adapter = new TestableCacheAdapter($client);

        self::assertTrue($adapter->exposedDoDelete(['a', 'b', 'c']));
        self::assertSame('DELETE FROM public.cache_items WHERE item_id IN ($1, $2, $3)', $client->executedQueries[0]['sql']);
        self::assertSame(['a', 'b', 'c'], $client->executedQueries[0]['parameters']);
    }

    public function test_delete_with_empty_id_list_is_a_noop() : void
    {
        $client = new SpyClient();
        $adapter = new TestableCacheAdapter($client);

        self::assertTrue($adapter->exposedDoDelete([]));
        self::assertSame([], $client->executedQueries);
    }

    public function test_fetch_deletes_expired_rows_after_yield() : void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [
            ['item_id' => 'fresh', 'item_data' => 'spy:s:1:"v";'],
            ['item_id' => 'stale', 'item_data' => null],
        ];
        $adapter = new TestableCacheAdapter($client, marshaller: new SpyMarshaller());

        \iterator_to_array($adapter->exposedDoFetch(['fresh', 'stale']));

        self::assertCount(2, $client->executedQueries);
        self::assertStringStartsWith('DELETE FROM public.cache_items WHERE item_id IN', $client->executedQueries[1]['sql']);
        self::assertSame(['stale'], $client->executedQueries[1]['parameters']);
    }

    public function test_fetch_emits_id_to_value_pairs_for_non_expired_rows() : void
    {
        $client = new SpyClient();
        $client->fetchAllReturn = [
            ['item_id' => 'a', 'item_data' => 'spy:s:5:"hello";'],
            ['item_id' => 'b', 'item_data' => 'spy:i:42;'],
        ];
        $marshaller = new SpyMarshaller();
        $adapter = new TestableCacheAdapter($client, marshaller: $marshaller);

        $out = \iterator_to_array($adapter->exposedDoFetch(['a', 'b']));

        self::assertSame(['a' => 'hello', 'b' => 42], $out);
        self::assertSame(2, $marshaller->unmarshallCalls);

        $sql = $client->executedQueries[0]['sql'];
        self::assertStringContainsString('SELECT item_id, CASE WHEN', $sql);
        self::assertStringContainsString('item_lifetime IS NULL', $sql);
        self::assertStringContainsString('item_lifetime + item_time', $sql);
        self::assertStringContainsString('FROM public.cache_items', $sql);
        self::assertStringContainsString('WHERE item_id IN ($2, $3)', $sql);
    }

    public function test_fetch_with_empty_ids_is_a_noop() : void
    {
        $client = new SpyClient();
        $adapter = new TestableCacheAdapter($client);

        self::assertSame([], \iterator_to_array($adapter->exposedDoFetch([])));
        self::assertSame([], $client->executedQueries);
    }

    public function test_have_returns_false_when_row_missing() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = null;
        $adapter = new TestableCacheAdapter($client);

        self::assertFalse($adapter->exposedDoHave('missing'));
    }

    public function test_have_returns_true_when_row_present_and_not_expired() : void
    {
        $client = new SpyClient();
        $client->fetchReturn = ['?column?' => 1];
        $adapter = new TestableCacheAdapter($client);

        self::assertTrue($adapter->exposedDoHave('present'));

        $sql = $client->executedQueries[0]['sql'];
        self::assertStringContainsString('SELECT 1', $sql);
        self::assertStringContainsString('FROM public.cache_items', $sql);
        self::assertStringContainsString('WHERE item_id = $1', $sql);
        self::assertStringContainsString('item_lifetime IS NULL', $sql);
        self::assertStringContainsString('LIMIT 1', $sql);
        self::assertSame('present', $client->executedQueries[0]['parameters'][0]);
    }

    public function test_marshaller_failures_are_returned_as_failed_ids() : void
    {
        $client = new SpyClient();
        $marshaller = new SpyMarshaller(failKeys: ['bad']);
        $adapter = new TestableCacheAdapter($client, marshaller: $marshaller);

        $failed = $adapter->exposedDoSave(['bad' => 'whatever'], 0);

        self::assertSame(['bad'], $failed);
        self::assertSame([], $client->executedQueries);
    }

    public function test_namespace_with_invalid_chars_throws_invalid_argument() : void
    {
        $this->expectException(InvalidArgumentException::class);

        new TestableCacheAdapter(new SpyClient(), namespace: 'not allowed!');
    }

    public function test_save_uses_custom_table_schema_and_columns_from_options() : void
    {
        $client = new SpyClient();
        $adapter = new TestableCacheAdapter($client, options: [
            'db_table' => 'cache',
            'db_schema' => 'app',
            'db_id_col' => 'k',
            'db_data_col' => 'v',
            'db_lifetime_col' => 'ttl',
            'db_time_col' => 'ts',
        ], marshaller: new SpyMarshaller());

        $adapter->exposedDoSave(['k1' => 'v1'], 60);

        $sql = $client->executedQueries[0]['sql'];
        self::assertStringContainsString('INSERT INTO app.cache (k, v, ttl, ts)', $sql);
        self::assertStringContainsString('ON CONFLICT (k) DO UPDATE', $sql);
        self::assertStringContainsString('v = excluded.v', $sql);
        self::assertStringContainsString('ttl = excluded.ttl', $sql);
        self::assertStringContainsString('ts = excluded.ts', $sql);
    }

    public function test_save_uses_insert_on_conflict_update_via_dsl() : void
    {
        $client = new SpyClient();
        $marshaller = new SpyMarshaller();
        $adapter = new TestableCacheAdapter($client, marshaller: $marshaller);

        $adapter->exposedDoSave(['key1' => 'value1'], 600);

        self::assertSame(1, $client->transactionCallCount);
        self::assertCount(1, $client->executedQueries);

        $sql = $client->executedQueries[0]['sql'];
        self::assertStringContainsString('INSERT INTO public.cache_items', $sql);
        self::assertStringContainsString('(item_id, item_data, item_lifetime, item_time)', $sql);
        self::assertStringContainsString('VALUES ($1, $2, $3, $4)', $sql);
        self::assertStringContainsString('ON CONFLICT (item_id) DO UPDATE', $sql);
        self::assertStringContainsString('item_data = excluded.item_data', $sql);
        self::assertStringContainsString('item_lifetime = excluded.item_lifetime', $sql);
        self::assertStringContainsString('item_time = excluded.item_time', $sql);

        self::assertSame('key1', $client->executedQueries[0]['parameters'][0]);
        self::assertSame(600, $client->executedQueries[0]['parameters'][2]);
    }

    public function test_save_with_zero_lifetime_uses_null_for_lifetime_column() : void
    {
        $client = new SpyClient();
        $adapter = new TestableCacheAdapter($client, marshaller: new SpyMarshaller());

        $adapter->exposedDoSave(['k' => 'v'], 0);

        self::assertNull($client->executedQueries[0]['parameters'][2]);
    }
}
