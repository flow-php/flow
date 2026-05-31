<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\exclude_exact;
use function Flow\PostgreSql\DSL\exclude_scoped;
use function Flow\PostgreSql\DSL\exclude_starts_with;

final class ScopedExclusionPolicyTest extends TestCase
{
    public function test_delegates_when_type_and_schema_match(): void
    {
        $policy = exclude_scoped(exclude_exact('uploads'), SchemaObjectType::TABLE, 'public');

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'uploads')));
    }

    public function test_blocks_when_type_differs(): void
    {
        $policy = exclude_scoped(exclude_exact('uploads'), SchemaObjectType::TABLE);

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SEQUENCE, 'public', 'uploads')));
    }

    public function test_blocks_when_schema_differs(): void
    {
        $policy = exclude_scoped(exclude_starts_with('upload_'), null, 'public');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'tenant_data', 'upload_1')));
    }

    public function test_delegates_fully_when_no_scope_is_set(): void
    {
        $policy = exclude_scoped(exclude_exact('uploads'));

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::VIEW, 'tenant_data', 'uploads')));
        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::VIEW, 'tenant_data', 'users')));
    }
}
