<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\Extension;
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

    public function test_normalize_and_from_array_round_trip_schema(): void
    {
        static::assertEquals(
            schema_extension('pgcrypto', '1.3', 'extp'),
            Extension::fromArray(schema_extension('pgcrypto', '1.3', 'extp')->normalize()),
        );
        static::assertSame('public', Extension::fromArray(['name' => 'pgcrypto', 'version' => null])->schema);
    }

    public function test_to_sql_defaults_to_public_schema(): void
    {
        static::assertSame('CREATE EXTENSION pgcrypto SCHEMA public', schema_extension('pgcrypto')->toSql()->toSql());
    }

    public function test_to_sql_generates_create_extension(): void
    {
        static::assertSame(
            'CREATE EXTENSION "uuid-ossp" SCHEMA public',
            schema_extension('uuid-ossp')->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_create_extension_with_version(): void
    {
        static::assertSame(
            'CREATE EXTENSION postgis SCHEMA public VERSION "3.4.0"',
            schema_extension('postgis', '3.4.0')->toSql()->toSql(),
        );
    }

    public function test_to_sql_renders_its_own_schema(): void
    {
        static::assertSame(
            'CREATE EXTENSION pgcrypto SCHEMA extp',
            schema_extension('pgcrypto', schema: 'extp')->toSql()->toSql(),
        );
    }
}
