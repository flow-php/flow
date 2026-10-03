<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowPhpExtension;
use Flow\ETL\Tests\Double\ForeignColumnStub;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_schema;

final class AdaptiveBackendTest extends TestCase
{
    protected function setUp(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('with flow_php AdaptiveBackend is RustBackend');
        }
    }

    public function test_adopt_is_what_the_php_backend_adopts(): void
    {
        $column = ColumnMother::of(int_schema('a'), [1, 2]);
        $foreign = new ForeignColumnStub($column);

        static::assertSame($column, (new AdaptiveBackend())->adopt(int_schema('a'), $column));
        static::assertEquals(
            (new PhpBackend())->adopt(int_schema('a'), $foreign),
            (new AdaptiveBackend())->adopt(int_schema('a'), $foreign),
        );
    }

    public function test_adopt_refuses_an_untyped_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "a": mixed cannot be a batch column');

        (new AdaptiveBackend())->adopt(int_schema('a'), new ValueColumn([1]));
    }

    public function test_allocated_bytes_is_what_the_php_backend_allocates(): void
    {
        static::assertSame((new PhpBackend())->allocatedBytes(), (new AdaptiveBackend())->allocatedBytes());
    }

    public function test_builder_builds_what_the_php_backend_builds(): void
    {
        $default = (new AdaptiveBackend())->builder(str_schema('a'));
        $default->append('x');
        $php = (new PhpBackend())->builder(str_schema('a'));
        $php->append('x');

        static::assertEquals($php->finish(), $default->finish());
    }

    public function test_constant_is_what_the_php_backend_builds(): void
    {
        static::assertEquals(
            (new PhpBackend())->constant(int_schema('a'), 7, 3),
            (new AdaptiveBackend())->constant(int_schema('a'), 7, 3),
        );
    }

    public function test_decode_is_what_the_php_backend_decodes(): void
    {
        $buffers = ColumnMother::of(int_schema('a', nullable: true), [1, null])->encode();

        static::assertEquals(
            (new PhpBackend())->decode(int_schema('a', nullable: true), $buffers, 2, 1),
            (new AdaptiveBackend())->decode(int_schema('a', nullable: true), $buffers, 2, 1),
        );
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        new AdaptiveBackend(new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'));
    }
}
