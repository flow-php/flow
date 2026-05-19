<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemCache\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemCache\FlowFilesystemCacheAdapter;
use Flow\Bridge\Symfony\FilesystemCache\Tests\Context\FilesystemCacheContext;
use PHPUnit\Framework\TestCase;

use function sleep;

final class FlowFilesystemCacheAdapterTest extends TestCase
{
    private FilesystemCacheContext $context;

    protected function setUp(): void
    {
        $this->context = new FilesystemCacheContext();
    }

    protected function tearDown(): void
    {
        $this->context->cleanup();
    }

    public function test_clear_with_namespace_only_deletes_matching_keys(): void
    {
        $appPool = new FlowFilesystemCacheAdapter(
            $this->context->filesystem,
            $this->context->directory,
            namespace: 'app',
        );
        $otherPool = new FlowFilesystemCacheAdapter(
            $this->context->filesystem,
            $this->context->directory,
            namespace: 'other',
        );

        $appItem = $appPool->getItem('one');
        $appItem->set('app-one');
        $appPool->save($appItem);

        $otherItem = $otherPool->getItem('one');
        $otherItem->set('other-one');
        $otherPool->save($otherItem);

        static::assertTrue($appPool->clear());

        static::assertFalse($appPool->getItem('one')->isHit());
        static::assertTrue($otherPool->getItem('one')->isHit());
    }

    public function test_delete_removes_value(): void
    {
        $adapter = new FlowFilesystemCacheAdapter($this->context->filesystem, $this->context->directory);

        $item = $adapter->getItem('to_delete');
        $item->set('present');
        $adapter->save($item);

        static::assertTrue($adapter->getItem('to_delete')->isHit());
        static::assertTrue($adapter->deleteItem('to_delete'));
        static::assertFalse($adapter->getItem('to_delete')->isHit());
    }

    public function test_get_returns_miss_for_unknown_key(): void
    {
        $adapter = new FlowFilesystemCacheAdapter($this->context->filesystem, $this->context->directory);

        static::assertFalse($adapter->getItem('never_saved')->isHit());
    }

    public function test_prune_removes_expired_items(): void
    {
        $adapter = new FlowFilesystemCacheAdapter($this->context->filesystem, $this->context->directory);

        $expired = $adapter->getItem('will_expire');
        $expired->set('gone');
        $expired->expiresAfter(1);
        $adapter->save($expired);

        $alive = $adapter->getItem('still_here');
        $alive->set('alive');
        $adapter->save($alive);

        sleep(2);

        static::assertTrue($adapter->prune());

        static::assertFalse($adapter->getItem('will_expire')->isHit());
        static::assertTrue($adapter->getItem('still_here')->isHit());
    }

    public function test_save_overwrites_existing_value(): void
    {
        $adapter = new FlowFilesystemCacheAdapter($this->context->filesystem, $this->context->directory);

        $first = $adapter->getItem('key');
        $first->set('first');
        $adapter->save($first);

        $second = $adapter->getItem('key');
        $second->set('second');
        $adapter->save($second);

        static::assertSame('second', $adapter->getItem('key')->get());
    }

    public function test_save_then_get_round_trip(): void
    {
        $adapter = new FlowFilesystemCacheAdapter($this->context->filesystem, $this->context->directory);

        $item = $adapter->getItem('hello');
        $item->set(['greeting' => 'world', 'count' => 7]);
        static::assertTrue($adapter->save($item));

        $fetched = $adapter->getItem('hello');
        static::assertTrue($fetched->isHit());
        static::assertSame(['greeting' => 'world', 'count' => 7], $fetched->get());
    }
}
