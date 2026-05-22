<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit;

use Flow\Bridge\Symfony\PostgreSQLCache\Tests\Context\PostgreSqlCacheContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Exception\InvalidArgumentException;

use function iterator_to_array;

final class FlowPostgreSqlCacheAdapterTest extends TestCase
{
    private PostgreSqlCacheContext $context;

    protected function setUp(): void
    {
        $this->context = new PostgreSqlCacheContext();
    }

    public function test_clear_with_empty_prefix_truncates_table(): void
    {
        $adapter = $this->context->adapter();

        static::assertTrue($adapter->clear());
        static::assertCount(1, $this->context->client->executedQueries);
        static::assertSame('TRUNCATE public.cache_items', $this->context->client->executedQueries[0]['sql']);
        static::assertSame([], $this->context->client->executedQueries[0]['parameters']);
    }

    public function test_clear_with_prefix_uses_like_pattern(): void
    {
        $adapter = $this->context->adapter();

        static::assertTrue($adapter->clear('user-'));
        static::assertSame(
            'DELETE FROM public.cache_items WHERE item_id LIKE $1',
            $this->context->client->executedQueries[0]['sql'],
        );
        static::assertSame(['user-%'], $this->context->client->executedQueries[0]['parameters']);
    }

    public function test_custom_marshaller_is_used_when_supplied(): void
    {
        $marshaller = $this->context->spyMarshaller();
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $item = $adapter->getItem('foo');
        $item->set('bar');
        $adapter->save($item);

        static::assertSame(1, $marshaller->marshallCalls);
        static::assertArrayHasKey('foo', $marshaller->marshalled);
    }

    public function test_delete_items_uses_in_clause(): void
    {
        $adapter = $this->context->adapter();

        static::assertTrue($adapter->deleteItems(['a', 'b', 'c']));
        static::assertSame(
            'DELETE FROM public.cache_items WHERE item_id IN ($1, $2, $3)',
            $this->context->client->executedQueries[0]['sql'],
        );
        static::assertSame(['a', 'b', 'c'], $this->context->client->executedQueries[0]['parameters']);
    }

    public function test_delete_items_with_empty_list_is_a_noop(): void
    {
        $adapter = $this->context->adapter();

        static::assertTrue($adapter->deleteItems([]));
        static::assertSame([], $this->context->client->executedQueries);
    }

    public function test_get_items_deletes_expired_rows_after_iteration(): void
    {
        $this->context->client->fetchAllReturn = [
            ['item_id' => 'fresh', 'item_data' => 'spy:s:1:"v";'],
            ['item_id' => 'stale', 'item_data' => null],
        ];
        $adapter = $this->context->adapter(marshaller: $this->context->spyMarshaller());

        iterator_to_array($adapter->getItems(['fresh', 'stale']));

        static::assertCount(2, $this->context->client->executedQueries);
        static::assertStringStartsWith(
            'DELETE FROM public.cache_items WHERE item_id IN',
            $this->context->client->executedQueries[1]['sql'],
        );
        static::assertSame(['stale'], $this->context->client->executedQueries[1]['parameters']);
    }

    public function test_get_items_emits_id_to_value_pairs_for_non_expired_rows(): void
    {
        $this->context->client->fetchAllReturn = [
            ['item_id' => 'a', 'item_data' => 'spy:s:5:"hello";'],
            ['item_id' => 'b', 'item_data' => 'spy:i:42;'],
        ];
        $marshaller = $this->context->spyMarshaller();
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $items = iterator_to_array($adapter->getItems(['a', 'b']));

        static::assertSame('hello', $items['a']->get());
        static::assertSame(42, $items['b']->get());
        static::assertSame(2, $marshaller->unmarshallCalls);

        $sql = $this->context->client->executedQueries[0]['sql'];
        static::assertStringContainsString('SELECT item_id, CASE WHEN', $sql);
        static::assertStringContainsString('item_lifetime IS NULL', $sql);
        static::assertStringContainsString('item_lifetime + item_time', $sql);
        static::assertStringContainsString('FROM public.cache_items', $sql);
        static::assertStringContainsString('WHERE item_id IN ($2, $3)', $sql);
    }

    public function test_get_items_with_empty_keys_is_a_noop(): void
    {
        $adapter = $this->context->adapter();

        static::assertSame([], iterator_to_array($adapter->getItems([])));
        static::assertSame([], $this->context->client->executedQueries);
    }

    public function test_has_item_returns_false_when_row_missing(): void
    {
        $this->context->client->fetchReturn = null;
        $adapter = $this->context->adapter();

        static::assertFalse($adapter->hasItem('missing'));
    }

    public function test_has_item_returns_true_when_row_present_and_not_expired(): void
    {
        $this->context->client->fetchReturn = ['?column?' => 1];
        $adapter = $this->context->adapter();

        static::assertTrue($adapter->hasItem('present'));

        $sql = $this->context->client->executedQueries[0]['sql'];
        static::assertStringContainsString('SELECT 1', $sql);
        static::assertStringContainsString('FROM public.cache_items', $sql);
        static::assertStringContainsString('WHERE item_id = $1', $sql);
        static::assertStringContainsString('item_lifetime IS NULL', $sql);
        static::assertStringContainsString('LIMIT 1', $sql);
        static::assertSame('present', $this->context->client->executedQueries[0]['parameters'][0]);
    }

    public function test_namespace_with_invalid_chars_throws_invalid_argument(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->context->adapter(namespace: 'not allowed!');
    }

    public function test_save_deferred_returns_failed_keys_for_marshaller_failures(): void
    {
        $adapter = $this->context->adapter(marshaller: $this->context->spyMarshaller('bad'));

        $bad = $adapter->getItem('bad');
        $bad->set('x');
        $good = $adapter->getItem('good');
        $good->set('y');

        $insertQueries = $this->context->onlyInsertQueries(static function () use ($adapter, $bad, $good): void {
            $adapter->saveDeferred($bad);
            $adapter->saveDeferred($good);
            self::assertFalse($adapter->commit());
        });

        static::assertCount(1, $insertQueries);
        static::assertSame('good', $insertQueries[0]['parameters'][0]);
    }

    public function test_save_returns_false_when_marshaller_fails_for_only_key(): void
    {
        $adapter = $this->context->adapter(marshaller: $this->context->spyMarshaller('bad'));

        $item = $adapter->getItem('bad');
        $item->set('whatever');

        $insertQueries = $this->context->onlyInsertQueries(static function () use ($adapter, $item): void {
            self::assertFalse($adapter->save($item));
        });

        static::assertSame([], $insertQueries);
    }

    public function test_save_uses_custom_table_schema_and_columns_from_options(): void
    {
        $adapter = $this->context->adapter(options: [
            'db_table' => 'cache',
            'db_schema' => 'app',
            'db_id_col' => 'k',
            'db_data_col' => 'v',
            'db_lifetime_col' => 'ttl',
            'db_time_col' => 'ts',
        ], marshaller: $this->context->spyMarshaller());

        $item = $adapter->getItem('k1');
        $item->set('v1');
        $item->expiresAfter(60);

        $insertQueries = $this->context->onlyInsertQueries(static function () use ($adapter, $item): void {
            $adapter->save($item);
        });

        static::assertCount(1, $insertQueries);
        $sql = $insertQueries[0]['sql'];
        static::assertStringContainsString('INSERT INTO app.cache (k, v, ttl, ts)', $sql);
        static::assertStringContainsString('ON CONFLICT (k) DO UPDATE', $sql);
        static::assertStringContainsString('v = excluded.v', $sql);
        static::assertStringContainsString('ttl = excluded.ttl', $sql);
        static::assertStringContainsString('ts = excluded.ts', $sql);
    }

    public function test_save_uses_insert_on_conflict_update_via_dsl(): void
    {
        $adapter = $this->context->adapter(marshaller: $this->context->spyMarshaller());

        $item = $adapter->getItem('key1');
        $item->set('value1');
        $item->expiresAfter(600);

        $insertQueries = $this->context->onlyInsertQueries(static function () use ($adapter, $item): void {
            $adapter->save($item);
        });

        static::assertSame(1, $this->context->client->transactionCallCount);
        static::assertCount(1, $insertQueries);

        $sql = $insertQueries[0]['sql'];
        static::assertStringContainsString('INSERT INTO public.cache_items', $sql);
        static::assertStringContainsString('(item_id, item_data, item_lifetime, item_time)', $sql);
        static::assertStringContainsString('VALUES ($1, $2, $3, $4)', $sql);
        static::assertStringContainsString('ON CONFLICT (item_id) DO UPDATE', $sql);
        static::assertStringContainsString('item_data = excluded.item_data', $sql);
        static::assertStringContainsString('item_lifetime = excluded.item_lifetime', $sql);
        static::assertStringContainsString('item_time = excluded.item_time', $sql);

        static::assertSame('key1', $insertQueries[0]['parameters'][0]);
        static::assertSame(600, $insertQueries[0]['parameters'][2]);
    }

    public function test_save_with_zero_lifetime_uses_null_for_lifetime_column(): void
    {
        $adapter = $this->context->adapter(marshaller: $this->context->spyMarshaller());

        $item = $adapter->getItem('k');
        $item->set('v');

        $insertQueries = $this->context->onlyInsertQueries(static function () use ($adapter, $item): void {
            $adapter->save($item);
        });

        static::assertCount(1, $insertQueries);
        static::assertNull($insertQueries[0]['parameters'][2]);
    }
}
