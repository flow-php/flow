<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\schema_domain;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Diff\DomainDiff;
use PHPUnit\Framework\TestCase;

final class DomainDiffTest extends TestCase
{
    public function test_adds_check_constraint() : void
    {
        $diff = new DomainDiff(
            schema_domain('positive_int', ColumnType::integer()),
            schema_domain('positive_int', ColumnType::integer()),
            addedCheckConstraints: [new CheckConstraint('value > 0', 'positive_check')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER DOMAIN positive_int ADD CONSTRAINT positive_check CHECK (value > 0)', $sqls[0]->toSql());
    }

    public function test_changes_default() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), default: null),
            schema_domain('email', ColumnType::text(), default: "'unknown'"),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame("ALTER DOMAIN email SET DEFAULT 'unknown'", $sqls[0]->toSql());
    }

    public function test_changes_nullable() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), nullable: true),
            schema_domain('email', ColumnType::text(), nullable: false),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER DOMAIN email SET NOT NULL', $sqls[0]->toSql());
    }

    public function test_drops_check_constraint() : void
    {
        $diff = new DomainDiff(
            schema_domain('positive_int', ColumnType::integer()),
            schema_domain('positive_int', ColumnType::integer()),
            removedCheckConstraints: [new CheckConstraint('value > 0', 'positive_check')],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER DOMAIN positive_int DROP CONSTRAINT positive_check', $sqls[0]->toSql());
    }

    public function test_drops_default() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), default: "'unknown'"),
            schema_domain('email', ColumnType::text(), default: null),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER DOMAIN email DROP DEFAULT', $sqls[0]->toSql());
    }

    public function test_has_base_type_changed_returns_false_when_same() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text()),
            schema_domain('email', ColumnType::text()),
        );

        self::assertFalse($diff->hasBaseTypeChanged());
    }

    public function test_has_base_type_changed_returns_true_when_different() : void
    {
        $diff = new DomainDiff(
            schema_domain('positive_int', ColumnType::integer()),
            schema_domain('positive_int', ColumnType::bigint()),
        );

        self::assertTrue($diff->hasBaseTypeChanged());
    }

    public function test_has_default_changed_returns_false_when_same() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), default: 'unknown'),
            schema_domain('email', ColumnType::text(), default: 'unknown'),
        );

        self::assertFalse($diff->hasDefaultChanged());
    }

    public function test_has_default_changed_returns_true_when_different() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), default: 'unknown'),
            schema_domain('email', ColumnType::text(), default: 'empty'),
        );

        self::assertTrue($diff->hasDefaultChanged());
    }

    public function test_has_nullable_changed_returns_false_when_same() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), nullable: false),
            schema_domain('email', ColumnType::text(), nullable: false),
        );

        self::assertFalse($diff->hasNullableChanged());
    }

    public function test_has_nullable_changed_returns_true_when_different() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), nullable: true),
            schema_domain('email', ColumnType::text(), nullable: false),
        );

        self::assertTrue($diff->hasNullableChanged());
    }

    public function test_recreates_when_base_type_changed() : void
    {
        $diff = new DomainDiff(
            schema_domain('positive_int', ColumnType::integer()),
            schema_domain('positive_int', ColumnType::bigint()),
        );

        $sqls = $diff->generate();

        self::assertCount(2, $sqls);
        self::assertSame('DROP DOMAIN positive_int CASCADE', $sqls[0]->toSql());
        self::assertSame('CREATE DOMAIN positive_int AS bigint', $sqls[1]->toSql());
    }

    public function test_recreates_with_all_properties_when_base_type_changed() : void
    {
        $diff = new DomainDiff(
            schema_domain('positive_int', ColumnType::integer(), nullable: false, default: '0', checkConstraints: [new CheckConstraint('value >= 0', 'non_negative')]),
            schema_domain('positive_int', ColumnType::bigint(), nullable: false, default: '0', checkConstraints: [new CheckConstraint('value >= 0', 'non_negative')]),
        );

        $sqls = $diff->generate();

        self::assertCount(2, $sqls);
        self::assertSame('DROP DOMAIN positive_int CASCADE', $sqls[0]->toSql());
        self::assertSame('CREATE DOMAIN positive_int AS bigint NOT NULL DEFAULT 0 CONSTRAINT non_negative CHECK (value >= 0)', $sqls[1]->toSql());
    }

    public function test_returns_empty_when_no_changes() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text()),
            schema_domain('email', ColumnType::text()),
        );

        self::assertSame([], $diff->generate());
    }

    public function test_reversed_nullable_change() : void
    {
        $diff = new DomainDiff(
            schema_domain('email', ColumnType::text(), nullable: false),
            schema_domain('email', ColumnType::text(), nullable: true),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('ALTER DOMAIN email DROP NOT NULL', $sqls[0]->toSql());
    }

    public function test_throws_when_adding_unnamed_constraint() : void
    {
        $diff = new DomainDiff(
            schema_domain('positive_int', ColumnType::integer()),
            schema_domain('positive_int', ColumnType::integer()),
            addedCheckConstraints: [new CheckConstraint('value > 0')],
        );

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot add unnamed check constraint on domain "positive_int"');
        $diff->generate();
    }

    public function test_throws_when_dropping_unnamed_constraint() : void
    {
        $diff = new DomainDiff(
            schema_domain('positive_int', ColumnType::integer()),
            schema_domain('positive_int', ColumnType::integer()),
            removedCheckConstraints: [new CheckConstraint('value > 0')],
        );

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot drop unnamed check constraint on domain "positive_int"');
        $diff->generate();
    }
}
