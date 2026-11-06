<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Diff\ExtensionDiff;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\PostgreSql\DSL\schema_extension;

final class ExtensionDiffTest extends TestCase
{
    public function test_generates_alter_extension_update_to(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp', '1.0'), schema_extension('uuid-ossp', '1.1'));

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER EXTENSION "uuid-ossp" UPDATE TO "1.1"', $sqls[0]->toSql());
    }

    public function test_generates_set_schema_when_schema_changed(): void
    {
        $diff = new ExtensionDiff(schema_extension('pgcrypto'), schema_extension('pgcrypto', schema: 'extp'));

        static::assertTrue($diff->hasSchemaChanged());
        static::assertSame(
            ['ALTER EXTENSION pgcrypto SET SCHEMA extp'],
            array_map(static fn(Sql $sql): string => $sql->toSql(), $diff->generate()),
        );
    }

    public function test_has_version_changed_returns_false_when_both_null(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp'), schema_extension('uuid-ossp'));

        static::assertFalse($diff->hasVersionChanged());
    }

    public function test_has_version_changed_returns_false_when_same(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp', '1.0'), schema_extension('uuid-ossp', '1.0'));

        static::assertFalse($diff->hasVersionChanged());
    }

    public function test_has_version_changed_returns_false_when_target_unpinned(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp', '1.0'), schema_extension('uuid-ossp'));

        static::assertFalse($diff->hasVersionChanged());
    }

    public function test_has_version_changed_returns_true_when_different(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp', '1.0'), schema_extension('uuid-ossp', '1.1'));

        static::assertTrue($diff->hasVersionChanged());
    }

    public function test_has_version_changed_returns_true_when_null_to_value(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp'), schema_extension('uuid-ossp', '1.0'));

        static::assertTrue($diff->hasVersionChanged());
    }

    public function test_returns_empty_when_no_changes(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp', '1.0'), schema_extension('uuid-ossp', '1.0'));

        static::assertSame([], $diff->generate());
    }

    public function test_reversed_version_change(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp', '1.1'), schema_extension('uuid-ossp', '1.0'));

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('ALTER EXTENSION "uuid-ossp" UPDATE TO "1.0"', $sqls[0]->toSql());
    }

    public function test_target_version_null_generates_nothing(): void
    {
        $diff = new ExtensionDiff(schema_extension('uuid-ossp', '1.0'), schema_extension('uuid-ossp'));

        static::assertSame([], $diff->generate());
    }
}
