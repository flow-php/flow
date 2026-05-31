<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use PHPUnit\Framework\TestCase;

final class SchemaObjectTest extends TestCase
{
    public function test_named_object(): void
    {
        $object = new SchemaObject(SchemaObjectType::TABLE, 'public', 'users');

        static::assertSame(SchemaObjectType::TABLE, $object->type);
        static::assertSame('public', $object->schema);
        static::assertSame('users', $object->name);
    }

    public function test_schema_level_object_defaults_to_null_name(): void
    {
        $object = new SchemaObject(SchemaObjectType::SCHEMA, 'tenant_data');

        static::assertSame(SchemaObjectType::SCHEMA, $object->type);
        static::assertSame('tenant_data', $object->schema);
        static::assertNull($object->name);
    }
}
