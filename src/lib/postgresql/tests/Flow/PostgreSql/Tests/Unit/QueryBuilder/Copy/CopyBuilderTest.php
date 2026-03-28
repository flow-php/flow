<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Copy;

use function Flow\PostgreSql\DSL\{
    col,
    copy,
    select
};

use Flow\PostgreSql\QueryBuilder\Copy\{CopyFormat, CopyOnError};
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class CopyBuilderTest extends TestCase
{
    public function test_copy_from_basic() : void
    {
        $query = copy()
            ->from('users')
            ->file('/tmp/users.csv');

        self::assertSame("COPY users FROM '/tmp/users.csv'", $query->toSql());
    }

    public function test_copy_from_csv_with_header() : void
    {
        $query = copy()
            ->from('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, HEADER true)", $query->toSql());
    }

    public function test_copy_from_program() : void
    {
        $query = copy()
            ->from('logs')
            ->program('gunzip -c /var/log/app.log.gz');

        self::assertSame("COPY logs FROM PROGRAM 'gunzip -c /var/log/app.log.gz'", $query->toSql());
    }

    public function test_copy_from_stdin() : void
    {
        $query = copy()
            ->from('users')
            ->stdin()
            ->format(CopyFormat::CSV);

        self::assertSame('COPY users FROM STDIN CSV', $query->toSql());
    }

    public function test_copy_from_with_all_csv_options() : void
    {
        $query = copy()
            ->from('data')
            ->file('/tmp/data.csv')
            ->format(CopyFormat::CSV)
            ->withHeader()
            ->delimiter(';')
            ->nullAs('NULL')
            ->quote("'")
            ->escape('\\')
            ->encoding('UTF8');

        self::assertSame("COPY data FROM '/tmp/data.csv' WITH (FORMAT CSV, DELIMITER ';', NULL 'NULL', HEADER true, QUOTE '''', ESCAPE E'\\\\', ENCODING 'UTF8')", $query->toSql());
    }

    public function test_copy_from_with_columns() : void
    {
        $query = copy()
            ->from('users')
            ->columns('id', 'name', 'email')
            ->file('/tmp/users.csv');

        self::assertSame("COPY users(id, name, email) FROM '/tmp/users.csv'", $query->toSql());
    }

    public function test_copy_from_with_force_not_null() : void
    {
        $query = copy()
            ->from('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceNotNull('name', 'email');

        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, FORCE_NOT_NULL (name, email))", $query->toSql());
    }

    public function test_copy_from_with_on_error_ignore() : void
    {
        $query = copy()
            ->from('events')
            ->file('/tmp/events.csv')
            ->onError(CopyOnError::IGNORE);

        self::assertSame("COPY events FROM '/tmp/events.csv' WITH (on_error ignore)", $query->toSql());
    }

    public function test_copy_to_basic() : void
    {
        $query = copy()
            ->to('users')
            ->file('/tmp/users.csv');

        self::assertSame("COPY users TO '/tmp/users.csv'", $query->toSql());
    }

    public function test_copy_to_binary_format() : void
    {
        $query = copy()
            ->to('data')
            ->file('/tmp/data.bin')
            ->format(CopyFormat::BINARY);

        self::assertSame("COPY data TO '/tmp/data.bin' WITH (FORMAT BINARY)", $query->toSql());
    }

    public function test_copy_to_csv_with_header() : void
    {
        $query = copy()
            ->to('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        self::assertSame("COPY users TO '/tmp/users.csv' WITH (FORMAT CSV, HEADER true)", $query->toSql());
    }

    public function test_copy_to_program() : void
    {
        $query = copy()
            ->to('logs')
            ->program('gzip > /tmp/logs.csv.gz');

        self::assertSame("COPY logs TO PROGRAM 'gzip > /tmp/logs.csv.gz'", $query->toSql());
    }

    public function test_copy_to_stdout() : void
    {
        $query = copy()
            ->to('users')
            ->stdout()
            ->format(CopyFormat::CSV);

        self::assertSame('COPY users TO STDOUT CSV', $query->toSql());
    }

    public function test_copy_to_with_columns() : void
    {
        $query = copy()
            ->to('users')
            ->columns('id', 'name', 'email')
            ->file('/tmp/users.csv');

        self::assertSame("COPY users(id, name, email) TO '/tmp/users.csv'", $query->toSql());
    }

    public function test_copy_to_with_force_quote_all() : void
    {
        $query = copy()
            ->to('products')
            ->file('/tmp/products.csv')
            ->format(CopyFormat::CSV)
            ->forceQuoteAll();

        self::assertSame("COPY products TO '/tmp/products.csv' WITH (FORMAT CSV, FORCE_QUOTE *)", $query->toSql());
    }

    public function test_copy_to_with_force_quote_columns() : void
    {
        $query = copy()
            ->to('products')
            ->file('/tmp/products.csv')
            ->format(CopyFormat::CSV)
            ->forceQuote('name', 'description');

        self::assertSame("COPY products TO '/tmp/products.csv' CSV FORCE QUOTE name, description", $query->toSql());
    }

    public function test_copy_to_with_query() : void
    {
        $selectQuery = select()
            ->select(col('id'), col('name'))
            ->from(new Table('users'));

        $query = copy()
            ->toQuery($selectQuery)
            ->file('/tmp/active_users.csv')
            ->format(CopyFormat::CSV);

        self::assertSame("COPY (SELECT id, name FROM users) TO '/tmp/active_users.csv' CSV", $query->toSql());
    }

    public function test_copy_to_with_schema() : void
    {
        $query = copy()
            ->to('analytics.events')
            ->file('/tmp/events.csv');

        self::assertSame("COPY analytics.events TO '/tmp/events.csv'", $query->toSql());
    }
}
