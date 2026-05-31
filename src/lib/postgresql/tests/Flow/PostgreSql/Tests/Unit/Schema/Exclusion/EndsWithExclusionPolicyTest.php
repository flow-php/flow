<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\exclude_ends_with;

final class EndsWithExclusionPolicyTest extends TestCase
{
    public function test_excludes_object_whose_name_ends_with_suffix(): void
    {
        $policy = exclude_ends_with('_tmp');

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::VIEW, 'public', 'report_tmp')));
    }

    public function test_does_not_exclude_when_suffix_absent(): void
    {
        $policy = exclude_ends_with('_tmp');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::VIEW, 'public', 'report')));
    }

    public function test_does_not_exclude_schema_level_object(): void
    {
        $policy = exclude_ends_with('_data');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SCHEMA, 'tenant_data')));
    }
}
