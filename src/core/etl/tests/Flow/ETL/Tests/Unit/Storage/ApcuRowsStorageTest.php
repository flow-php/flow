<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage;

use APCUIterator;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Storage\ApcuRowsStorage;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function apcu_delete;
use function apcu_enabled;
use function apcu_store;
use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

#[CoversClass(ApcuRowsStorage::class)]
#[RequiresPhpExtension('apcu')]
final class ApcuRowsStorageTest extends FlowTestCase
{
    private ApcuRowsStorage $cache;

    private ApcuRowsStorage $otherCache;

    #[Override]
    protected function setUp(): void
    {
        if (!apcu_enabled()) {
            static::markTestSkipped('APCu is not enabled for CLI (apc.enable_cli=0).');
        }

        $this->cache = new ApcuRowsStorage(new FloeSerializer(new PhpBackend()), 'flow_php_cache_test');
        $this->otherCache = new ApcuRowsStorage(new FloeSerializer(new PhpBackend()), 'flow_php_cache_test_other');

        apcu_delete(new APCUIterator('/^flow_php_cache_test/'));
    }

    #[Override]
    protected function tearDown(): void
    {
        if (apcu_enabled()) {
            apcu_delete(new APCUIterator('/^flow_php_cache_test/'));
        }
    }

    public function test_appended_batches_read_back_in_order(): void
    {
        static::assertFalse($this->cache->has('rows'));

        $this->cache->append('rows', array_to_rows(
            [['id' => 1, 'name' => 'John']],
            schema(int_schema('id'), str_schema('name')),
        ));
        $this->cache->append('rows', array_to_rows(
            [['id' => 2, 'name' => 'Jane']],
            schema(int_schema('id'), str_schema('name')),
        ));

        static::assertTrue($this->cache->has('rows'));
        static::assertSame(
            [[['id' => 1, 'name' => 'John']], [['id' => 2, 'name' => 'Jane']]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array($this->cache->get('rows'), false),
            ),
        );
        static::assertSame(2, $this->cache->rows('rows')->exactly());
        static::assertEquals(schema(int_schema('id'), str_schema('name')), $this->cache->schema('rows'));
    }

    public function test_a_missing_key_has_no_rows_and_no_schema(): void
    {
        static::assertFalse($this->cache->has('missing'));
        static::assertSame([], iterator_to_array($this->cache->get('missing'), false));
        static::assertSame(0, $this->cache->rows('missing')->exactly());
        static::assertNull($this->cache->schema('missing'));
    }

    public function test_namespaces_do_not_share_keys(): void
    {
        $this->cache->append('rows', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertFalse($this->otherCache->has('rows'));
    }

    public function test_remove_drops_the_key_and_its_chunks(): void
    {
        $this->cache->append('first', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $this->cache->append('second', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        $this->cache->remove('second');
        $this->cache->remove('never-written');

        static::assertTrue($this->cache->has('first'));
        static::assertFalse($this->cache->has('second'));
        static::assertSame([], iterator_to_array($this->cache->get('second'), false));
    }

    public function test_a_chunk_another_writer_replaced_is_refused(): void
    {
        $this->cache->append('rows', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        apcu_store('flow_php_cache_test:rows:chunk:0', 42);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Rows under "rows" are corrupted or were not written by ApcuCache.');

        iterator_to_array($this->cache->get('rows'));
    }

    public function test_an_entry_another_writer_stored_reads_as_empty(): void
    {
        apcu_store('flow_php_cache_test:orphan', 'not an entry');

        static::assertTrue($this->cache->has('orphan'));
        static::assertSame([], iterator_to_array($this->cache->get('orphan'), false));
        static::assertEquals(schema(), $this->cache->schema('orphan'));
    }
}
