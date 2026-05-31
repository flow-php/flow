<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\exclude_schema;

final class WholeSchemaExclusionPolicyTest extends TestCase
{
    public function test_excludes_the_schema_itself(): void
    {
        $policy = exclude_schema('tenant_data');

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::SCHEMA, 'tenant_data')));
    }

    public function test_excludes_every_object_inside_the_schema(): void
    {
        $policy = exclude_schema('tenant_data');

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'tenant_data', 'uploads')));
        static::assertTrue($policy->exclude(new SchemaObject(
            SchemaObjectType::SEQUENCE,
            'tenant_data',
            'uploads_id_seq',
        )));
    }

    public function test_does_not_exclude_objects_in_other_schemas(): void
    {
        $policy = exclude_schema('tenant_data');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'uploads')));
        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SCHEMA, 'public')));
    }
}
