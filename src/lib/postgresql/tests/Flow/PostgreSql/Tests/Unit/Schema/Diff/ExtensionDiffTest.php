<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\schema_extension;

use Flow\PostgreSql\Schema\Diff\ExtensionDiff;
use PHPUnit\Framework\TestCase;

final class ExtensionDiffTest extends TestCase
{
    public function test_generates_alter_extension_update_to() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp', '1.0'),
            schema_extension('uuid-ossp', '1.1'),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER EXTENSION "uuid-ossp" UPDATE TO "1.1"', $sqls[0]->toSql());
    }

    public function test_generates_alter_extension_update_when_target_version_null() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp', '1.0'),
            schema_extension('uuid-ossp'),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER EXTENSION "uuid-ossp" UPDATE', $sqls[0]->toSql());
    }

    public function test_has_version_changed_returns_false_when_both_null() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp'),
            schema_extension('uuid-ossp'),
        );

        self::assertFalse($diff->hasVersionChanged());
    }

    public function test_has_version_changed_returns_false_when_same() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp', '1.0'),
            schema_extension('uuid-ossp', '1.0'),
        );

        self::assertFalse($diff->hasVersionChanged());
    }

    public function test_has_version_changed_returns_true_when_different() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp', '1.0'),
            schema_extension('uuid-ossp', '1.1'),
        );

        self::assertTrue($diff->hasVersionChanged());
    }

    public function test_has_version_changed_returns_true_when_null_to_value() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp'),
            schema_extension('uuid-ossp', '1.0'),
        );

        self::assertTrue($diff->hasVersionChanged());
    }

    public function test_returns_empty_when_no_changes() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp', '1.0'),
            schema_extension('uuid-ossp', '1.0'),
        );

        self::assertSame([], $diff->generate());
    }

    public function test_reversed_version_change() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp', '1.1'),
            schema_extension('uuid-ossp', '1.0'),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER EXTENSION "uuid-ossp" UPDATE TO "1.0"', $sqls[0]->toSql());
    }

    public function test_reversed_version_change_when_target_version_null() : void
    {
        $diff = new ExtensionDiff(
            schema_extension('uuid-ossp', '1.0'),
            schema_extension('uuid-ossp'),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER EXTENSION "uuid-ossp" UPDATE', $sqls[0]->toSql());
    }
}
