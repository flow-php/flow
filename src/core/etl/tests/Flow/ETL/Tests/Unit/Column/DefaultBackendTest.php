<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_schema;

final class DefaultBackendTest extends TestCase
{
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
