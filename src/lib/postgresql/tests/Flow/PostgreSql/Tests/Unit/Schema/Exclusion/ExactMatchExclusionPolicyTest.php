<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\exclude_exact;

final class ExactMatchExclusionPolicyTest extends TestCase
{
    public function test_excludes_matching_name_regardless_of_type(): void
    {
        $policy = exclude_exact('audit_log');

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'audit_log')));
        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::SEQUENCE, 'public', 'audit_log')));
    }

    public function test_does_not_exclude_different_name(): void
    {
        $policy = exclude_exact('audit_log');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'users')));
    }

    public function test_does_not_exclude_schema_level_object(): void
    {
        $policy = exclude_exact('audit_log');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SCHEMA, 'audit_log')));
    }
}
