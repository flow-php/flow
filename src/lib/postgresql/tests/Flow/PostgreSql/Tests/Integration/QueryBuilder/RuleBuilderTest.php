<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use function Flow\PostgreSql\DSL\{create, drop};
use Flow\PostgreSql\Tests\Integration\QueryBuilder\Assertions\QueryBuilderAssertions;
use PHPUnit\Framework\TestCase;

final class RuleBuilderTest extends TestCase
{
    use QueryBuilderAssertions;

    public function test_create_rule_do_also_insert() : void
    {
        $builder = create()->rule('audit_insert')
            ->asOnInsert()
            ->to('users')
            ->doAlso("INSERT INTO audit_log (action) VALUES ('insert')");

        $this->assertCreateRuleQuery($builder, "CREATE RULE audit_insert AS ON INSERT TO users DO INSERT INTO audit_log (action) VALUES ('insert')");
    }

    public function test_create_rule_do_instead_delete() : void
    {
        $builder = create()->rule('soft_delete')
            ->asOnDelete()
            ->to('users')
            ->doInstead('UPDATE users SET deleted = true WHERE id = OLD.id');

        $this->assertCreateRuleQuery($builder, 'CREATE RULE soft_delete AS ON DELETE TO users DO INSTEAD UPDATE users SET deleted = true WHERE id = old.id');
    }

    public function test_create_rule_do_nothing() : void
    {
        $builder = create()->rule('prevent_delete')
            ->asOnDelete()
            ->to('users')
            ->doNothing();

        $this->assertCreateRuleQuery($builder, 'CREATE RULE prevent_delete AS ON DELETE TO users DO NOTHING');
    }

    public function test_create_rule_on_select() : void
    {
        $builder = create()->rule('redirect_select')
            ->asOnSelect()
            ->to('old_table')
            ->doInstead('SELECT * FROM new_table');

        $this->assertCreateRuleQuery($builder, 'CREATE RULE redirect_select AS ON SELECT TO old_table DO INSTEAD SELECT * FROM new_table');
    }

    public function test_create_rule_on_update() : void
    {
        $builder = create()->rule('track_update')
            ->asOnUpdate()
            ->to('users')
            ->doAlso("INSERT INTO update_log (table_name) VALUES ('users')");

        $this->assertCreateRuleQuery($builder, "CREATE RULE track_update AS ON UPDATE TO users DO INSERT INTO update_log (table_name) VALUES ('users')");
    }

    public function test_create_rule_or_replace() : void
    {
        $builder = create()->rule('prevent_delete')
            ->orReplace()
            ->asOnDelete()
            ->to('users')
            ->doNothing();

        $this->assertCreateRuleQuery($builder, 'CREATE OR REPLACE RULE prevent_delete AS ON DELETE TO users DO NOTHING');
    }

    public function test_create_rule_with_schema() : void
    {
        $builder = create()->rule('prevent_delete')
            ->asOnDelete()
            ->to('public.users')
            ->doNothing();

        $this->assertCreateRuleQuery($builder, 'CREATE RULE prevent_delete AS ON DELETE TO public.users DO NOTHING');
    }

    public function test_create_rule_with_where_condition() : void
    {
        $builder = create()->rule('protect_admin')
            ->asOnDelete()
            ->to('users')
            ->where('OLD.role = \'admin\'')
            ->doNothing();

        $this->assertCreateRuleQuery($builder, "CREATE RULE protect_admin AS ON DELETE TO users WHERE old.role = 'admin' DO NOTHING");
    }

    public function test_drop_rule() : void
    {
        $builder = drop()->rule('prevent_delete')
            ->on('users');

        $this->assertDropRuleQuery($builder, 'DROP RULE prevent_delete ON users');
    }

    public function test_drop_rule_cascade() : void
    {
        $builder = drop()->rule('prevent_delete')
            ->on('users')
            ->cascade();

        $this->assertDropRuleQuery($builder, 'DROP RULE prevent_delete ON users CASCADE');
    }

    public function test_drop_rule_if_exists() : void
    {
        $builder = drop()->rule('prevent_delete')
            ->ifExists()
            ->on('users');

        $this->assertDropRuleQuery($builder, 'DROP RULE IF EXISTS prevent_delete ON users');
    }

    public function test_drop_rule_with_schema() : void
    {
        $builder = drop()->rule('prevent_delete')
            ->on('public.users')
            ->restrict();

        $this->assertDropRuleQuery($builder, 'DROP RULE prevent_delete ON public.users');
    }
}
