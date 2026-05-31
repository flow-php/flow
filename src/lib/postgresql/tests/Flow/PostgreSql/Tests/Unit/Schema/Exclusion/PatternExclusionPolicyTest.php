<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\PatternExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\exclude_pattern;

final class PatternExclusionPolicyTest extends TestCase
{
    public function test_excludes_object_matching_pattern(): void
    {
        $policy = exclude_pattern('/^cache_\d+$/');

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::SEQUENCE, 'public', 'cache_128')));
    }

    public function test_does_not_exclude_non_matching_object(): void
    {
        $policy = exclude_pattern('/^cache_\d+$/');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SEQUENCE, 'public', 'cache_items')));
    }

    public function test_does_not_exclude_schema_level_object(): void
    {
        $policy = exclude_pattern('/^tenant_/');

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SCHEMA, 'tenant_data')));
    }

    public function test_invalid_pattern_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new PatternExclusionPolicy('/unterminated');
    }
}
