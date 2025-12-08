<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter_role, create_role, drop_owned, drop_role, grant, grant_role, reassign_owned, reset_role, revoke, revoke_role, set_role};

use Flow\PgQuery\QueryBuilder\Schema\Grant\TablePrivilege;

final class RoleBuilderTest extends PGQueryTestCase
{
    public function test_alter_role_rename() : void
    {
        $builder = alter_role('old_name')
            ->renameTo('new_name');

        $this->assertAlterRoleRenameQuery(
            $builder,
            'ALTER ROLE old_name RENAME TO new_name'
        );
    }

    public function test_alter_role_set_options() : void
    {
        $builder = alter_role('admin')
            ->superuser();

        $this->assertAlterRoleQuery(
            $builder,
            'ALTER ROLE admin WITH SUPERUSER'
        );
    }

    public function test_alter_role_with_multiple_options() : void
    {
        $builder = alter_role('admin')
            ->noSuperuser()
            ->createDb();

        $this->assertAlterRoleQuery(
            $builder,
            'ALTER ROLE admin WITH NOSUPERUSER CREATEDB'
        );
    }

    public function test_create_role_simple() : void
    {
        $builder = create_role('admin');

        $this->assertCreateRoleQuery(
            $builder,
            'CREATE ROLE admin'
        );
    }

    public function test_create_role_with_login() : void
    {
        $builder = create_role('app_user')
            ->login();

        $this->assertCreateRoleQuery(
            $builder,
            'CREATE ROLE app_user WITH LOGIN'
        );
    }

    public function test_create_role_with_options() : void
    {
        $builder = create_role('admin')
            ->superuser()
            ->login();

        $this->assertCreateRoleQuery(
            $builder,
            'CREATE ROLE admin WITH SUPERUSER LOGIN'
        );
    }

    public function test_create_user_with_password() : void
    {
        $builder = create_role('app_user')
            ->login()
            ->withPassword('secret');

        $this->assertCreateRoleQuery(
            $builder,
            "CREATE ROLE app_user WITH LOGIN PASSWORD 'secret'"
        );
    }

    public function test_drop_owned_by() : void
    {
        $builder = drop_owned('role1');

        $this->assertDropOwnedQuery(
            $builder,
            'DROP OWNED BY role1'
        );
    }

    public function test_drop_owned_by_cascade() : void
    {
        $builder = drop_owned('role1')
            ->cascade();

        $this->assertDropOwnedQuery(
            $builder,
            'DROP OWNED BY role1 CASCADE'
        );
    }

    public function test_drop_role_if_exists() : void
    {
        $builder = drop_role('admin')
            ->ifExists();

        $this->assertDropRoleQuery(
            $builder,
            'DROP ROLE IF EXISTS admin'
        );
    }

    public function test_drop_role_multiple() : void
    {
        $builder = drop_role('role1', 'role2');

        $this->assertDropRoleQuery(
            $builder,
            'DROP ROLE role1, role2'
        );
    }

    public function test_drop_role_simple() : void
    {
        $builder = drop_role('admin');

        $this->assertDropRoleQuery(
            $builder,
            'DROP ROLE admin'
        );
    }

    public function test_grant_all_on_table() : void
    {
        $builder = grant(TablePrivilege::ALL)
            ->onTable('users')
            ->to('admin');

        $this->assertGrantQuery(
            $builder,
            'GRANT ALL ON users TO admin'
        );
    }

    public function test_grant_on_all_tables_in_schema() : void
    {
        $builder = grant(TablePrivilege::SELECT)
            ->onAllTablesInSchema('public')
            ->to('admin');

        $this->assertGrantQuery(
            $builder,
            'GRANT select ON ALL TABLES IN SCHEMA public TO admin'
        );
    }

    public function test_grant_role_to_role() : void
    {
        $builder = grant_role('admin')
            ->to('user1');

        $this->assertGrantRoleQuery(
            $builder,
            'GRANT admin TO user1'
        );
    }

    public function test_grant_role_with_admin_option() : void
    {
        $builder = grant_role('admin')
            ->to('user1')
            ->withAdminOption();

        $this->assertGrantRoleQuery(
            $builder,
            'GRANT admin TO user1 WITH ADMIN OPTION'
        );
    }

    public function test_grant_select_on_table() : void
    {
        $builder = grant(TablePrivilege::SELECT)
            ->onTable('users')
            ->to('app_user');

        $this->assertGrantQuery(
            $builder,
            'GRANT select ON users TO app_user'
        );
    }

    public function test_grant_with_grant_option() : void
    {
        $builder = grant(TablePrivilege::SELECT)
            ->onTable('users')
            ->to('app_user')
            ->withGrantOption();

        $this->assertGrantQuery(
            $builder,
            'GRANT select ON users TO app_user WITH GRANT OPTION'
        );
    }

    public function test_reassign_owned_by() : void
    {
        $builder = reassign_owned('old_role')
            ->to('new_role');

        $this->assertReassignOwnedQuery(
            $builder,
            'REASSIGN OWNED BY old_role TO new_role'
        );
    }

    public function test_reset_role() : void
    {
        $builder = reset_role();

        $this->assertResetRoleQuery(
            $builder,
            'RESET role'
        );
    }

    public function test_revoke_role_cascade() : void
    {
        $builder = revoke_role('admin')
            ->from('user1')
            ->cascade();

        $this->assertRevokeRoleQuery(
            $builder,
            'REVOKE admin FROM user1 CASCADE'
        );
    }

    public function test_revoke_role_from_role() : void
    {
        $builder = revoke_role('admin')
            ->from('user1');

        $this->assertRevokeRoleQuery(
            $builder,
            'REVOKE admin FROM user1'
        );
    }

    public function test_revoke_select_on_table() : void
    {
        $builder = revoke(TablePrivilege::SELECT)
            ->onTable('users')
            ->from('app_user');

        $this->assertRevokeQuery(
            $builder,
            'REVOKE select ON users FROM app_user'
        );
    }

    public function test_revoke_with_cascade() : void
    {
        $builder = revoke(TablePrivilege::SELECT)
            ->onTable('users')
            ->from('app_user')
            ->cascade();

        $this->assertRevokeQuery(
            $builder,
            'REVOKE select ON users FROM app_user CASCADE'
        );
    }

    public function test_set_role() : void
    {
        $builder = set_role('admin');

        $this->assertSetRoleQuery(
            $builder,
            'SET role TO admin'
        );
    }
}
