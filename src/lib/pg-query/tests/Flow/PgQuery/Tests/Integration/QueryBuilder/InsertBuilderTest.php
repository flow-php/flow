<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    pg_bool,
    pg_col,
    pg_conflict_columns,
    pg_conflict_constraint,
    pg_eq,
    pg_insert,
    pg_param,
    pg_select,
    pg_string,
    pg_table
};

final class InsertBuilderTest extends PGQueryTestCase
{
    public function test_insert_on_conflict_do_update_with_where() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('email', 'name', 'active')
            ->values(pg_string('john@example.com'), pg_string('John'), pg_bool(true))
            ->onConflictDoUpdate(
                pg_conflict_columns(['email']),
                ['name' => pg_string('Updated John')]
            )
            ->where(pg_eq(pg_col('users.active'), pg_bool(true)));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (email, name, active) VALUES ('john@example.com', 'John', true) ON CONFLICT (email) DO UPDATE SET name = 'Updated John' WHERE users.active = true"
        );
    }

    public function test_insert_on_conflict_on_constraint() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(pg_string('John'), pg_string('john@example.com'))
            ->onConflictDoNothing(pg_conflict_constraint('users_pkey'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING"
        );
    }

    public function test_insert_select() : void
    {
        $selectQuery = pg_select()
            ->select(pg_col('name'), pg_col('email'))
            ->from(pg_table('archived_users'));

        $query = pg_insert()
            ->into('users')
            ->columns('name', 'email')
            ->select($selectQuery);

        $this->assertInsertQueryRoundTrip(
            $query,
            'INSERT INTO users (name, email) SELECT name, email FROM archived_users'
        );
    }

    public function test_insert_with_default_values() : void
    {
        $query = pg_insert()
            ->into('users')
            ->defaultValues();

        $this->assertInsertQueryRoundTrip(
            $query,
            'INSERT INTO users DEFAULT VALUES'
        );
    }

    public function test_insert_with_multiple_rows() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(pg_string('John'), pg_string('john@example.com'))
            ->values(pg_string('Jane'), pg_string('jane@example.com'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')"
        );
    }

    public function test_insert_with_on_conflict_do_nothing() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(pg_string('John'), pg_string('john@example.com'))
            ->onConflictDoNothing();

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT DO NOTHING"
        );
    }

    public function test_insert_with_on_conflict_do_update() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('email', 'name')
            ->values(pg_string('john@example.com'), pg_string('John'))
            ->onConflictDoUpdate(
                pg_conflict_columns(['email']),
                ['name' => pg_string('Updated John')]
            );

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO UPDATE SET name = 'Updated John'"
        );
    }

    public function test_insert_with_on_conflict_on_columns_do_nothing() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(pg_string('John'), pg_string('john@example.com'))
            ->onConflictDoNothing(pg_conflict_columns(['email']));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT (email) DO NOTHING"
        );
    }

    public function test_insert_with_parameters() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(pg_param(1), pg_param(2));

        $this->assertInsertQueryRoundTrip(
            $query,
            'INSERT INTO users (name, email) VALUES ($1, $2)'
        );
    }

    public function test_insert_with_returning() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name')
            ->values(pg_string('John'))
            ->returning(pg_col('id'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name) VALUES ('John') RETURNING id"
        );
    }

    public function test_insert_with_returning_all() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name')
            ->values(pg_string('John'))
            ->returningAll();

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name) VALUES ('John') RETURNING *"
        );
    }

    public function test_insert_with_schema_qualified_table() : void
    {
        $query = pg_insert()
            ->into('public.users')
            ->columns('name')
            ->values(pg_string('John'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO public.users (name) VALUES ('John')"
        );
    }

    public function test_simple_insert_with_values() : void
    {
        $query = pg_insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(pg_string('John'), pg_string('john@example.com'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
        );
    }
}
