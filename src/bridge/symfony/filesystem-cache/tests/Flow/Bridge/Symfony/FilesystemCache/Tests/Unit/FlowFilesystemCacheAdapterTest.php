<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemCache\Tests\Unit;

use Flow\Bridge\Symfony\FilesystemCache\Exception\FilesystemCacheException;
use Flow\Bridge\Symfony\FilesystemCache\Tests\Context\FilesystemCacheContext;
use Flow\Bridge\Symfony\FilesystemCache\Tests\Unit\Double\{FailingMvFilesystem, SpyLogger, SpyMarshaller};
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Exception\InvalidArgumentException;
use Symfony\Component\Cache\Marshaller\DefaultMarshaller;

final class FlowFilesystemCacheAdapterTest extends TestCase
{
    private FilesystemCacheContext $context;

    protected function setUp() : void
    {
        $this->context = new FilesystemCacheContext();
    }

    protected function tearDown() : void
    {
        $this->context->cleanup();
    }

    public function test_clear_with_empty_prefix_removes_all_items() : void
    {
        $adapter = $this->context->adapter();
        $a = $adapter->getItem('a');
        $a->set(1);
        $adapter->save($a);
        $b = $adapter->getItem('b');
        $b->set(2);
        $adapter->save($b);

        self::assertCount(2, $this->context->listFiles());
        self::assertTrue($adapter->clear());

        self::assertFalse($adapter->getItem('a')->isHit());
        self::assertFalse($adapter->getItem('b')->isHit());
    }

    public function test_clear_with_prefix_only_removes_items_whose_id_starts_with_prefix() : void
    {
        $adapter = $this->context->adapter();
        $kept = $adapter->getItem('keep_one');
        $kept->set('keep');
        $adapter->save($kept);
        $dropped = $adapter->getItem('drop_one');
        $dropped->set('drop');
        $adapter->save($dropped);

        self::assertTrue($adapter->clear('drop_'));

        self::assertTrue($adapter->getItem('keep_one')->isHit());
        self::assertFalse($adapter->getItem('drop_one')->isHit());
    }

    public function test_delete_removes_existing_item() : void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('to_delete');
        $item->set('present');
        $adapter->save($item);

        self::assertTrue($adapter->hasItem('to_delete'));
        self::assertTrue($adapter->deleteItem('to_delete'));
        self::assertFalse($adapter->hasItem('to_delete'));
    }

    public function test_delete_treats_missing_item_as_success() : void
    {
        self::assertTrue($this->context->adapter()->deleteItem('never_saved'));
    }

    public function test_get_item_for_corrupted_file_returns_miss_and_logs_corruption() : void
    {
        $adapter = $this->context->adapter();
        $logger = new SpyLogger();
        $adapter->setLogger($logger);

        $item = $adapter->getItem('broken');
        $item->set('original');
        $adapter->save($item);

        $this->context->corruptOnlyFile('this-is-not-three-lines');

        self::assertFalse($adapter->getItem('broken')->isHit());
        self::assertNotEmpty($logger->records);
        self::assertInstanceOf(FilesystemCacheException::class, $logger->records[0]['context']['exception'] ?? null);
        self::assertStringContainsString('is corrupted', $logger->records[0]['context']['exception']->getMessage());
    }

    public function test_get_item_for_expired_entry_returns_miss_and_deletes_file() : void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('expired');
        $item->set('gone');
        $adapter->save($item);

        $this->context->corruptOnlyFile(\sprintf('%010d', \time() - 60) . "\nexpired\nspy:s:4:\"gone\";");

        self::assertFalse($adapter->getItem('expired')->isHit());
        self::assertCount(0, $this->context->listFiles());
    }

    public function test_get_items_returns_hits_for_saved_keys() : void
    {
        $adapter = $this->context->adapter();
        $hello = $adapter->getItem('hello');
        $hello->set('world');
        $adapter->save($hello);
        $count = $adapter->getItem('count');
        $count->set(7);
        $adapter->save($count);

        $items = \iterator_to_array($adapter->getItems(['hello', 'count']));

        self::assertSame('world', $items['hello']->get());
        self::assertSame(7, $items['count']->get());
    }

    public function test_has_item_returns_false_for_expired_entry() : void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('stale');
        $item->set('body');
        $adapter->save($item);

        $this->context->corruptOnlyFile(\sprintf('%010d', \time() - 1) . "\nstale\nbody");

        self::assertFalse($adapter->hasItem('stale'));
    }

    public function test_has_item_returns_false_for_missing_file() : void
    {
        self::assertFalse($this->context->adapter()->hasItem('nope'));
    }

    public function test_has_item_returns_true_for_existing_non_expired_entry() : void
    {
        $adapter = $this->context->adapter(defaultLifetime: 60);
        $item = $adapter->getItem('fresh');
        $item->set('v');
        $adapter->save($item);

        self::assertTrue($adapter->hasItem('fresh'));
    }

    public function test_namespace_with_invalid_chars_throws() : void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->context->adapter(namespace: 'not allowed!');
    }

    public function test_prune_removes_only_expired_entries() : void
    {
        $marshaller = new SpyMarshaller();
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $expired = $adapter->getItem('expired');
        $expired->set('gone');
        $adapter->save($expired);
        $this->context->corruptOnlyFile(\sprintf('%010d', \time() - 60) . "\nexpired\nspy:s:4:\"gone\";");

        $alive = $adapter->getItem('alive');
        $alive->set('here');
        $alive->expiresAfter(600);
        $adapter->save($alive);

        $forever = $adapter->getItem('forever');
        $forever->set('always');
        $adapter->save($forever);

        self::assertTrue($adapter->prune());
        self::assertCount(2, $this->context->listFiles());
        self::assertTrue($adapter->hasItem('alive'));
        self::assertTrue($adapter->hasItem('forever'));
    }

    public function test_save_deferred_returns_failed_keys_for_marshaller_failures() : void
    {
        $marshaller = new SpyMarshaller(failKeys: ['bad']);
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $bad = $adapter->getItem('bad');
        $bad->set('x');
        $good = $adapter->getItem('good');
        $good->set('y');

        $adapter->saveDeferred($bad);
        $adapter->saveDeferred($good);
        self::assertFalse($adapter->commit());

        self::assertFalse($adapter->getItem('bad')->isHit());
        self::assertSame('y', $adapter->getItem('good')->get());
    }

    public function test_save_returns_false_and_logs_when_mv_fails() : void
    {
        $adapter = $this->context->adapter(filesystem: new FailingMvFilesystem($this->context->filesystem));
        $logger = new SpyLogger();
        $adapter->setLogger($logger);

        $item = $adapter->getItem('key');
        $item->set('value');

        self::assertFalse($adapter->save($item));
        self::assertNotEmpty($logger->records);
        self::assertInstanceOf(FilesystemCacheException::class, $logger->records[0]['context']['exception'] ?? null);
        self::assertStringContainsString('mv returned false', $logger->records[0]['context']['exception']->getMessage());
    }

    public function test_save_returns_false_when_marshaller_fails_for_only_key() : void
    {
        $marshaller = new SpyMarshaller(failKeys: ['only']);
        $adapter = $this->context->adapter(marshaller: $marshaller);

        $only = $adapter->getItem('only');
        $only->set('x');

        self::assertFalse($adapter->save($only));
        self::assertSame([], $this->context->listFiles());
    }

    public function test_save_with_zero_lifetime_writes_zero_expiry_header() : void
    {
        $adapter = $this->context->adapter();
        $item = $adapter->getItem('k');
        $item->set('v');
        $adapter->save($item);

        $parts = \explode("\n", $this->context->readOnlyFile(), 3);
        self::assertCount(3, $parts);
        self::assertSame('0000000000', $parts[0]);
        self::assertSame('k', $parts[1]);
    }

    public function test_save_writes_file_with_three_line_format() : void
    {
        $marshaller = new DefaultMarshaller(useIgbinarySerialize: false);
        $adapter = $this->context->adapter(marshaller: $marshaller);
        $item = $adapter->getItem('key1');
        $item->set('value1');
        $item->expiresAfter(600);
        $adapter->save($item);

        $parts = \explode("\n", $this->context->readOnlyFile(), 3);
        self::assertCount(3, $parts);
        self::assertSame(10, \strlen($parts[0]));
        self::assertGreaterThanOrEqual(\time() + 599, (int) $parts[0]);
        self::assertSame('key1', $parts[1]);
        self::assertSame('s:6:"value1";', $parts[2]);
    }
}
