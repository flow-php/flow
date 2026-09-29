<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\Php\ValueColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Tests\Double\ForeignColumnStub;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_schema;

final class DefaultBackendTest extends TestCase
{
    protected function setUp(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php registers its own DefaultBackend');
        }
    }

    public function test_adopt_is_what_the_php_backend_adopts(): void
    {
        $column = ColumnMother::of(int_schema('a'), [1, 2]);
        $foreign = new ForeignColumnStub($column);

        static::assertSame($column, (new DefaultBackend())->adopt(int_schema('a'), $column));
        static::assertEquals(
            (new PhpBackend())->adopt(int_schema('a'), $foreign),
            (new DefaultBackend())->adopt(int_schema('a'), $foreign),
        );
    }

    public function test_adopt_refuses_an_untyped_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "a": mixed cannot be a batch column');

        (new DefaultBackend())->adopt(int_schema('a'), new ValueColumn([1]));
    }

    public function test_allocated_bytes_is_what_the_php_backend_allocates(): void
    {
        static::assertSame((new PhpBackend())->allocatedBytes(), (new DefaultBackend())->allocatedBytes());
    }

    public function test_builder_builds_what_the_php_backend_builds(): void
    {
        $default = (new DefaultBackend())->builder(str_schema('a'));
        $default->append('x');
        $php = (new PhpBackend())->builder(str_schema('a'));
        $php->append('x');

        static::assertEquals($php->finish(), $default->finish());
    }

    public function test_constant_is_what_the_php_backend_builds(): void
    {
        static::assertEquals(
            (new PhpBackend())->constant(int_schema('a'), 7, 3),
            (new DefaultBackend())->constant(int_schema('a'), 7, 3),
        );
    }

    public function test_decode_is_what_the_php_backend_decodes(): void
    {
        $buffers = ColumnMother::of(int_schema('a', nullable: true), [1, null])->encode();

        static::assertEquals(
            (new PhpBackend())->decode(int_schema('a', nullable: true), $buffers, 2, 1),
            (new DefaultBackend())->decode(int_schema('a', nullable: true), $buffers, 2, 1),
        );
    }
}
