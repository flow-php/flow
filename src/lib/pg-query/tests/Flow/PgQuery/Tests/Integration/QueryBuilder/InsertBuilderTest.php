<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    col,
    conflict_columns,
    conflict_constraint,
    eq,
    insert,
    literal,
    param,
    select,
    table
};

final class InsertBuilderTest extends PGQueryTestCase
{
    public function test_insert_on_conflict_do_update_with_where() : void
    {
        $query = insert()
            ->into('users')
            ->columns('email', 'name', 'active')
            ->values(literal('john@example.com'), literal('John'), literal(true))
            ->onConflictDoUpdate(
                conflict_columns(['email']),
                ['name' => literal('Updated John')]
            )
            ->where(eq(col('users.active'), literal(true)));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (email, name, active) VALUES ('john@example.com', 'John', true) ON CONFLICT (email) DO UPDATE SET name = 'Updated John' WHERE users.active = true"
        );
    }

    public function test_insert_on_conflict_on_constraint() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(literal('John'), literal('john@example.com'))
            ->onConflictDoNothing(conflict_constraint('users_pkey'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING"
        );
    }

    public function test_insert_select() : void
    {
        $selectQuery = select()
            ->select(col('name'), col('email'))
            ->from(table('archived_users'));

        $query = insert()
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
        $query = insert()
            ->into('users')
            ->defaultValues();

        $this->assertInsertQueryRoundTrip(
            $query,
            'INSERT INTO users DEFAULT VALUES'
        );
    }

    public function test_insert_with_multiple_rows() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(literal('John'), literal('john@example.com'))
            ->values(literal('Jane'), literal('jane@example.com'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')"
        );
    }

    public function test_insert_with_on_conflict_do_nothing() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(literal('John'), literal('john@example.com'))
            ->onConflictDoNothing();

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT DO NOTHING"
        );
    }

    public function test_insert_with_on_conflict_do_update() : void
    {
        $query = insert()
            ->into('users')
            ->columns('email', 'name')
            ->values(literal('john@example.com'), literal('John'))
            ->onConflictDoUpdate(
                conflict_columns(['email']),
                ['name' => literal('Updated John')]
            );

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO UPDATE SET name = 'Updated John'"
        );
    }

    public function test_insert_with_on_conflict_on_columns_do_nothing() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(literal('John'), literal('john@example.com'))
            ->onConflictDoNothing(conflict_columns(['email']));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT (email) DO NOTHING"
        );
    }

    public function test_insert_with_parameters() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(param(1), param(2));

        $this->assertInsertQueryRoundTrip(
            $query,
            'INSERT INTO users (name, email) VALUES ($1, $2)'
        );
    }

    public function test_insert_with_returning() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name')
            ->values(literal('John'))
            ->returning(col('id'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name) VALUES ('John') RETURNING id"
        );
    }

    public function test_insert_with_returning_all() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name')
            ->values(literal('John'))
            ->returningAll();

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name) VALUES ('John') RETURNING *"
        );
    }

    public function test_insert_with_schema_qualified_table() : void
    {
        $query = insert()
            ->into('public.users')
            ->columns('name')
            ->values(literal('John'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO public.users (name) VALUES ('John')"
        );
    }

    public function test_simple_insert_with_values() : void
    {
        $query = insert()
            ->into('users')
            ->columns('name', 'email')
            ->values(literal('John'), literal('john@example.com'));

        $this->assertInsertQueryRoundTrip(
            $query,
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
        );
    }
}
