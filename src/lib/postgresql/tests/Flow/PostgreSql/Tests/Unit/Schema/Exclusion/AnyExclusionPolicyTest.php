<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Exclusion;

use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\exclude_any;
use function Flow\PostgreSql\DSL\exclude_ends_with;
use function Flow\PostgreSql\DSL\exclude_starts_with;

final class AnyExclusionPolicyTest extends TestCase
{
    public function test_excludes_when_any_child_policy_excludes(): void
    {
        $policy = exclude_any(exclude_starts_with('tmp_'), exclude_ends_with('_archive'));

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'tmp_orders')));
        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'orders_archive')));
    }

    public function test_does_not_exclude_when_no_child_matches(): void
    {
        $policy = exclude_any(exclude_starts_with('tmp_'), exclude_ends_with('_archive'));

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'orders')));
    }

    public function test_empty_policy_excludes_nothing(): void
    {
        $policy = exclude_any();

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'orders')));
    }
}
