<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_extension;

final class ExtensionTest extends TestCase
{
    public function test_extension_construction(): void
    {
        $ext = schema_extension('uuid-ossp');

        static::assertSame('uuid-ossp', $ext->name);
        static::assertNull($ext->version);
    }

    public function test_extension_with_version(): void
    {
        $ext = schema_extension('postgis', '3.4.0');

        static::assertSame('postgis', $ext->name);
        static::assertSame('3.4.0', $ext->version);
    }

    public function test_to_sql_generates_create_extension(): void
    {
        static::assertSame('CREATE EXTENSION "uuid-ossp"', schema_extension('uuid-ossp')->toSql()->toSql());
    }

    public function test_to_sql_generates_create_extension_with_version(): void
    {
        static::assertSame(
            'CREATE EXTENSION postgis VERSION "3.4.0"',
            schema_extension('postgis', '3.4.0')->toSql()->toSql(),
        );
    }
}
