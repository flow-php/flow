<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\schema_extension;

use PHPUnit\Framework\TestCase;

final class ExtensionTest extends TestCase
{
    public function test_extension_construction() : void
    {
        $ext = schema_extension('uuid-ossp');

        self::assertSame('uuid-ossp', $ext->name);
        self::assertNull($ext->version);
    }

    public function test_extension_with_version() : void
    {
        $ext = schema_extension('postgis', '3.4.0');

        self::assertSame('postgis', $ext->name);
        self::assertSame('3.4.0', $ext->version);
    }

    public function test_to_sql_generates_create_extension() : void
    {
        self::assertSame(
            'CREATE EXTENSION "uuid-ossp"',
            schema_extension('uuid-ossp')->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_create_extension_with_version() : void
    {
        self::assertSame(
            'CREATE EXTENSION postgis VERSION "3.4.0"',
            schema_extension('postgis', '3.4.0')->toSql()->toSql(),
        );
    }
}
