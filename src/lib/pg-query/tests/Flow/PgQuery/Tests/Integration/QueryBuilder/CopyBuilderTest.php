<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    col,
    copy,
    select
};

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{CopyStmt, Node, RawStmt};
use Flow\PgQuery\QueryBuilder\Copy\{CopyFormat, CopyFromFinalStep, CopyOnError, CopyToFinalStep};
use Flow\PgQuery\QueryBuilder\Table\Table;

final class CopyBuilderTest extends PGQueryTestCase
{
    public function test_copy_from_basic() : void
    {
        $query = copy()
            ->from('users')
            ->file('/tmp/users.csv');

        $this->assertCopyFromQueryEquals(
            $query,
            "COPY users FROM '/tmp/users.csv'"
        );
    }

    public function test_copy_from_csv_with_header() : void
    {
        $query = copy()
            ->from('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        $this->assertCopyFromQueryEquals(
            $query,
            "COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, HEADER true)"
        );
    }

    public function test_copy_from_program() : void
    {
        $query = copy()
            ->from('logs')
            ->program('gunzip -c /var/log/app.log.gz');

        $this->assertCopyFromQueryEquals(
            $query,
            "COPY logs FROM PROGRAM 'gunzip -c /var/log/app.log.gz'"
        );
    }

    public function test_copy_from_stdin() : void
    {
        $query = copy()
            ->from('users')
            ->stdin()
            ->format(CopyFormat::CSV);

        $this->assertCopyFromQueryEquals(
            $query,
            'COPY users FROM STDIN CSV'
        );
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

        $this->assertCopyFromQueryEquals(
            $query,
            "COPY data FROM '/tmp/data.csv' WITH (FORMAT CSV, DELIMITER ';', NULL 'NULL', HEADER true, QUOTE '''', ESCAPE E'\\\\', ENCODING 'UTF8')"
        );
    }

    public function test_copy_from_with_columns() : void
    {
        $query = copy()
            ->from('users')
            ->columns('id', 'name', 'email')
            ->file('/tmp/users.csv');

        $this->assertCopyFromQueryEquals(
            $query,
            "COPY users(id, name, email) FROM '/tmp/users.csv'"
        );
    }

    public function test_copy_from_with_force_not_null() : void
    {
        $query = copy()
            ->from('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceNotNull('name', 'email');

        $this->assertCopyFromQueryEquals(
            $query,
            "COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, FORCE_NOT_NULL (name, email))"
        );
    }

    public function test_copy_from_with_on_error_ignore() : void
    {
        $query = copy()
            ->from('events')
            ->file('/tmp/events.csv')
            ->onError(CopyOnError::IGNORE);

        $this->assertCopyFromQueryEquals(
            $query,
            "COPY events FROM '/tmp/events.csv' WITH (on_error ignore)"
        );
    }

    public function test_copy_to_basic() : void
    {
        $query = copy()
            ->to('users')
            ->file('/tmp/users.csv');

        $this->assertCopyToQueryEquals(
            $query,
            "COPY users TO '/tmp/users.csv'"
        );
    }

    public function test_copy_to_binary_format() : void
    {
        $query = copy()
            ->to('data')
            ->file('/tmp/data.bin')
            ->format(CopyFormat::BINARY);

        $this->assertCopyToQueryEquals(
            $query,
            "COPY data TO '/tmp/data.bin' WITH (FORMAT BINARY)"
        );
    }

    public function test_copy_to_csv_with_header() : void
    {
        $query = copy()
            ->to('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        $this->assertCopyToQueryEquals(
            $query,
            "COPY users TO '/tmp/users.csv' WITH (FORMAT CSV, HEADER true)"
        );
    }

    public function test_copy_to_program() : void
    {
        $query = copy()
            ->to('logs')
            ->program('gzip > /tmp/logs.csv.gz');

        $this->assertCopyToQueryEquals(
            $query,
            "COPY logs TO PROGRAM 'gzip > /tmp/logs.csv.gz'"
        );
    }

    public function test_copy_to_stdout() : void
    {
        $query = copy()
            ->to('users')
            ->stdout()
            ->format(CopyFormat::CSV);

        $this->assertCopyToQueryEquals(
            $query,
            'COPY users TO STDOUT CSV'
        );
    }

    public function test_copy_to_with_columns() : void
    {
        $query = copy()
            ->to('users')
            ->columns('id', 'name', 'email')
            ->file('/tmp/users.csv');

        $this->assertCopyToQueryEquals(
            $query,
            "COPY users(id, name, email) TO '/tmp/users.csv'"
        );
    }

    public function test_copy_to_with_force_quote_all() : void
    {
        $query = copy()
            ->to('products')
            ->file('/tmp/products.csv')
            ->format(CopyFormat::CSV)
            ->forceQuoteAll();

        $this->assertCopyToQueryEquals(
            $query,
            "COPY products TO '/tmp/products.csv' WITH (FORMAT CSV, FORCE_QUOTE *)"
        );
    }

    public function test_copy_to_with_force_quote_columns() : void
    {
        $query = copy()
            ->to('products')
            ->file('/tmp/products.csv')
            ->format(CopyFormat::CSV)
            ->forceQuote('name', 'description');

        $this->assertCopyToQueryEquals(
            $query,
            "COPY products TO '/tmp/products.csv' CSV FORCE QUOTE name, description"
        );
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

        $this->assertCopyToQueryEquals(
            $query,
            "COPY (SELECT id, name FROM users) TO '/tmp/active_users.csv' CSV"
        );
    }

    public function test_copy_to_with_schema() : void
    {
        $query = copy()
            ->to('analytics.events')
            ->file('/tmp/events.csv');

        $this->assertCopyToQueryEquals(
            $query,
            "COPY analytics.events TO '/tmp/events.csv'"
        );
    }

    protected function assertCopyFromQueryEquals(CopyFromFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCopyStmt($builder->toAst());

        self::assertSame($expectedSql, $sql);
    }

    protected function assertCopyToQueryEquals(CopyToFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCopyStmt($builder->toAst());

        self::assertSame($expectedSql, $sql);
    }

    protected function deparseCopyStmt(CopyStmt $copyStmt) : string
    {
        $node = new Node();
        $node->setCopyStmt($copyStmt);

        $parser = new Parser();
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
