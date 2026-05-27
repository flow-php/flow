<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemCache\Tests\Unit;

use Flow\Bridge\Symfony\FilesystemCache\Exception\FilesystemCacheException;
use Flow\Bridge\Symfony\FilesystemCache\Tests\Context\FilesystemCacheContext;
use Flow\Bridge\Symfony\FilesystemCache\Tests\Unit\Double\FailingMvFilesystem;
use Flow\Bridge\Symfony\FilesystemCache\Tests\Unit\Double\SpyLogger;
use Flow\Bridge\Symfony\FilesystemCache\Tests\Unit\Double\SpyMarshaller;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Exception\InvalidArgumentException;
use Symfony\Component\Cache\Marshaller\DefaultMarshaller;
use Symfony\Contracts\Cache\ItemInterface as CacheItemInterface;

use function explode;
use function iterator_to_array;
use function sprintf;
use function strlen;
use function time;

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

    public function test_clear_with_empty_prefix_removes_all_items(): void
    {
        $adapter = $this->context->adapter();
        $a = $adapter->getItem('a');
        $a->set(1);
        $adapter->save($a);
        $b = $adapter->getItem('b');
        $b->set(2);
        $adapter->save($b);

        static::assertCount(2, $this->context->listFiles());
        static::assertTrue($adapter->clear());

        static::assertFalse($adapter->getItem('a')->isHit());
        static::assertFalse($adapter->getItem('b')->isHit());
    }

    public function test_clear_with_prefix_only_removes_items_whose_id_starts_with_prefix(): void
    {
        $adapter = $this->context->adapter();
        $kept = $adapter->getItem('keep-one');
        $kept->set('keep');
        $adapter->save($kept);
        $dropped = $adapter->getItem('drop-one');
        $dropped->set('drop');
        $adapter->save($dropped);

        static::assertTrue($adapter->clear('drop-'));

        static::assertTrue($adapter->getItem('keep-one')->isHit());
        static::assertFalse($adapter->getItem('drop-one')->isHit());
    }

    public function test_delete_removes_existing_item(): void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('to_delete');
        $item->set('present');
        $adapter->save($item);

        static::assertTrue($adapter->hasItem('to_delete'));
        static::assertTrue($adapter->deleteItem('to_delete'));
        static::assertFalse($adapter->hasItem('to_delete'));
    }

    public function test_delete_treats_missing_item_as_success(): void
    {
        static::assertTrue($this->context->adapter()->deleteItem('never_saved'));
    }

    public function test_get_item_for_corrupted_file_returns_miss_and_logs_corruption(): void
    {
        $adapter = $this->context->adapter();
        $logger = new SpyLogger();
        $adapter->setLogger($logger);

        $item = $adapter->getItem('broken');
        $item->set('original');
        $adapter->save($item);

        $this->context->corruptOnlyFile('this-is-not-three-lines');

        static::assertFalse($adapter->getItem('broken')->isHit());
        static::assertNotEmpty($logger->records);
        static::assertInstanceOf(FilesystemCacheException::class, $logger->records[0]['context']['exception'] ?? null);
        static::assertStringContainsString('is corrupted', $logger->records[0]['context']['exception']->getMessage());
    }

    public function test_get_item_for_expired_entry_returns_miss_and_deletes_file(): void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('expired');
        $item->set('gone');
        $adapter->save($item);

        $this->context->corruptOnlyFile(sprintf('%010d', time() - 60) . "\nexpired\nspy:s:4:\"gone\";");

        static::assertFalse($adapter->getItem('expired')->isHit());
        static::assertCount(0, $this->context->listFiles());
    }

    public function test_get_items_returns_hits_for_saved_keys(): void
    {
        $adapter = $this->context->adapter();
        $hello = $adapter->getItem('hello');
        $hello->set('world');
        $adapter->save($hello);
        $count = $adapter->getItem('count');
        $count->set(7);
        $adapter->save($count);

        $items = iterator_to_array($adapter->getItems(['hello', 'count']));

        // @mago-expect analysis:mixed-assignment
        $helloItem = $items['hello'];
        // @mago-expect analysis:mixed-assignment
        $countItem = $items['count'];
        static::assertInstanceOf(CacheItemInterface::class, $helloItem);
        static::assertInstanceOf(CacheItemInterface::class, $countItem);
        static::assertSame('world', $helloItem->get());
        static::assertSame(7, $countItem->get());
    }

    public function test_has_item_returns_false_for_expired_entry(): void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('stale');
        $item->set('body');
        $adapter->save($item);

        $this->context->corruptOnlyFile(sprintf('%010d', time() - 1) . "\nstale\nbody");

        static::assertFalse($adapter->hasItem('stale'));
    }

    public function test_has_item_returns_false_for_missing_file(): void
    {
        static::assertFalse($this->context->adapter()->hasItem('nope'));
    }

    public function test_has_item_returns_true_for_existing_non_expired_entry(): void
    {
        $adapter = $this->context->adapter(defaultLifetime: 60);
        $item = $adapter->getItem('fresh');
        $item->set('v');
        $adapter->save($item);

        static::assertTrue($adapter->hasItem('fresh'));
    }

    public function test_namespace_with_invalid_chars_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->context->adapter(namespace: 'not allowed!');
    }

    public function test_prune_removes_only_expired_entries(): void
    {
        $marshaller = new SpyMarshaller();
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $expired = $adapter->getItem('expired');
        $expired->set('gone');
        $adapter->save($expired);
        $this->context->corruptOnlyFile(sprintf('%010d', time() - 60) . "\nexpired\nspy:s:4:\"gone\";");

        $alive = $adapter->getItem('alive');
        $alive->set('here');
        $alive->expiresAfter(600);
        $adapter->save($alive);

        $forever = $adapter->getItem('forever');
        $forever->set('always');
        $adapter->save($forever);

        static::assertTrue($adapter->prune());
        static::assertCount(2, $this->context->listFiles());
        static::assertTrue($adapter->hasItem('alive'));
        static::assertTrue($adapter->hasItem('forever'));
    }

    public function test_save_deferred_returns_failed_keys_for_marshaller_failures(): void
    {
        $marshaller = new SpyMarshaller(failKeys: ['bad']);
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $bad = $adapter->getItem('bad');
        $bad->set('x');
        $good = $adapter->getItem('good');
        $good->set('y');

        $adapter->saveDeferred($bad);
        $adapter->saveDeferred($good);
        static::assertFalse($adapter->commit());

        static::assertFalse($adapter->getItem('bad')->isHit());
        static::assertSame('y', $adapter->getItem('good')->get());
    }

    public function test_save_returns_false_and_logs_when_mv_fails(): void
    {
        $adapter = $this->context->adapter(filesystem: new FailingMvFilesystem($this->context->filesystem));
        $logger = new SpyLogger();
        $adapter->setLogger($logger);

        $item = $adapter->getItem('key');
        $item->set('value');

        static::assertFalse($adapter->save($item));
        static::assertNotEmpty($logger->records);
        static::assertInstanceOf(FilesystemCacheException::class, $logger->records[0]['context']['exception'] ?? null);
        static::assertStringContainsString(
            'mv returned false',
            $logger->records[0]['context']['exception']->getMessage(),
        );
    }

    public function test_save_returns_false_when_marshaller_fails_for_only_key(): void
    {
        $marshaller = new SpyMarshaller(failKeys: ['only']);
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $only = $adapter->getItem('only');
        $only->set('x');

        static::assertFalse($adapter->save($only));
        static::assertSame([], $this->context->listFiles());
    }

    public function test_save_with_zero_lifetime_writes_zero_expiry_header(): void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('k');
        $item->set('v');
        $adapter->save($item);

        $parts = explode("\n", $this->context->readOnlyFile(), 3);
        static::assertCount(3, $parts);
        static::assertSame('0000000000', $parts[0]);
        static::assertSame('k', $parts[1]);
    }

    public function test_save_writes_file_with_three_line_format(): void
    {
        $marshaller = new DefaultMarshaller(useIgbinarySerialize: false);
        $adapter = $this->context->adapter(marshaller: $marshaller);
        $item = $adapter->getItem('key1');
        $item->set('value1');
        $item->expiresAfter(600);
        $adapter->save($item);

        $parts = explode("\n", $this->context->readOnlyFile(), 3);
        static::assertCount(3, $parts);
        static::assertSame(10, strlen($parts[0]));
        static::assertGreaterThanOrEqual(time() + 599, (int) $parts[0]);
        static::assertSame('key1', $parts[1]);
        static::assertSame('s:6:"value1";', $parts[2]);
    }
}
