<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\exclude_starts_with;

final class StartsWithExclusionPolicyTest extends TestCase
{
    public function test_excludes_object_whose_name_starts_with_prefix(): void
    {
        $policy = exclude_starts_with('user_upload_');

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'user_upload_42')));
    }

    public function test_does_not_exclude_when_prefix_absent(): void
    {
        $policy = exclude_starts_with('user_upload_');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'users')));
    }

    public function test_does_not_exclude_schema_level_object(): void
    {
        $policy = exclude_starts_with('tenant_');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SCHEMA, 'tenant_data')));
    }
}
