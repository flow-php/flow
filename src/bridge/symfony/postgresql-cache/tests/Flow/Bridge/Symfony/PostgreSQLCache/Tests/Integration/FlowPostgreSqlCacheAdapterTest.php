<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSQLCache\FlowPostgreSqlCacheAdapter;

final class FlowPostgreSqlCacheAdapterTest extends CacheIntegrationTestCase
{
    public function test_clear_with_namespace_only_deletes_matching_keys() : void
    {
        $adapter = new FlowPostgreSqlCacheAdapter($this->cacheContext()->client, namespace: 'app');
        $other = new FlowPostgreSqlCacheAdapter($this->cacheContext()->client, namespace: 'other');

        $a = $adapter->getItem('one');
        $a->set('app-one');
        $adapter->save($a);

        $o = $other->getItem('one');
        $o->set('other-one');
        $other->save($o);

        self::assertTrue($adapter->clear());

        self::assertFalse($adapter->getItem('one')->isHit());
        self::assertTrue($other->getItem('one')->isHit());
    }

    public function test_delete_removes_value() : void
    {
        $adapter = new FlowPostgreSqlCacheAdapter($this->cacheContext()->client);

        $item = $adapter->getItem('to_delete');
        $item->set('present');
        $adapter->save($item);

        self::assertTrue($adapter->getItem('to_delete')->isHit());
        self::assertTrue($adapter->deleteItem('to_delete'));
        self::assertFalse($adapter->getItem('to_delete')->isHit());
    }

    public function test_get_returns_miss_for_unknown_key() : void
    {
        $adapter = new FlowPostgreSqlCacheAdapter($this->cacheContext()->client);

        self::assertFalse($adapter->getItem('never_saved')->isHit());
    }

    public function test_prune_removes_expired_items() : void
    {
        $adapter = new FlowPostgreSqlCacheAdapter($this->cacheContext()->client);

        $expired = $adapter->getItem('will_expire');
        $expired->set('gone');
        $expired->expiresAfter(1);
        $adapter->save($expired);

        $alive = $adapter->getItem('still_here');
        $alive->set('alive');
        $adapter->save($alive);

        \sleep(2);

        self::assertTrue($adapter->prune());

        self::assertFalse($adapter->getItem('will_expire')->isHit());
        self::assertTrue($adapter->getItem('still_here')->isHit());
    }

    public function test_save_overwrites_existing_value() : void
    {
        $adapter = new FlowPostgreSqlCacheAdapter($this->cacheContext()->client);

        $first = $adapter->getItem('key');
        $first->set('first');
        $adapter->save($first);

        $second = $adapter->getItem('key');
        $second->set('second');
        $adapter->save($second);

        self::assertSame('second', $adapter->getItem('key')->get());
    }

    public function test_save_then_get_round_trip() : void
    {
        $adapter = new FlowPostgreSqlCacheAdapter($this->cacheContext()->client);

        $item = $adapter->getItem('hello');
        $item->set(['greeting' => 'world', 'count' => 7]);
        self::assertTrue($adapter->save($item));

        $fetched = $adapter->getItem('hello');
        self::assertTrue($fetched->isHit());
        self::assertSame(['greeting' => 'world', 'count' => 7], $fetched->get());
    }
}
