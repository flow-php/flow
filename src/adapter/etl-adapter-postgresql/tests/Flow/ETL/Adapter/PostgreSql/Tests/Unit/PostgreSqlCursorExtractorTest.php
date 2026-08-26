<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlCursorExtractor;
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Cursor;
use Generator;
use PHPUnit\Framework\MockObject\MockObject;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class PostgreSqlCursorExtractorTest extends FlowTestCase
{
    public function test_cursor_loop_breaks_immediately_when_empty_result(): void
    {
        $client = $this->createClientMock();
        $cursor = $this->createCursorMock(rows: [], count: 0);

        $client->expects(self::once())->method('getTransactionNestingLevel')->willReturn(1);

        $client->expects(self::exactly(2))->method('execute');

        $client->expects(self::once())->method('cursor')->willReturn($cursor);

        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');
        $extractor = $extractor->withFetchSize(10);

        $rows = [];

        foreach ($extractor->extract($this->createFlowContext()) as $rowsData) {
            $rows[] = $rowsData;
        }

        static::assertSame([], $rows);
    }

    public function test_cursor_loop_breaks_when_rows_less_than_fetch_size(): void
    {
        $client = $this->createClientMock();

        $cursor = $this->createCursorMock(rows: [
            ['id' => 1, 'name' => 'User 1'],
            ['id' => 2, 'name' => 'User 2'],
            ['id' => 3, 'name' => 'User 3'],
        ], count: 3);

        $client->expects(self::once())->method('getTransactionNestingLevel')->willReturn(1);

        $client->expects(self::exactly(2))->method('execute');

        $client->expects(self::once())->method('cursor')->willReturn($cursor);

        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');
        $extractor = $extractor->withFetchSize(10);

        $rows = [];

        foreach ($extractor->extract($this->createFlowContext()) as $rowsData) {
            $rows = [...$rows, ...$rowsData->toArray()];
        }

        static::assertCount(3, $rows);
    }

    public function test_cursor_loop_fetches_multiple_batches_when_needed(): void
    {
        $client = $this->createClientMock();

        $cursor1 = $this->createCursorMock(rows: [
            ['id' => 1, 'name' => 'User 1'],
            ['id' => 2, 'name' => 'User 2'],
        ], count: 2);

        $cursor2 = $this->createCursorMock(rows: [
            ['id' => 3, 'name' => 'User 3'],
        ], count: 1);

        $client->expects(self::once())->method('getTransactionNestingLevel')->willReturn(1);

        $client->expects(self::exactly(2))->method('execute');

        $client->expects(self::exactly(2))->method('cursor')->willReturnOnConsecutiveCalls($cursor1, $cursor2);

        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');
        $extractor = $extractor->withFetchSize(2);

        $rows = [];

        foreach ($extractor->extract($this->createFlowContext()) as $rowsData) {
            $rows = [...$rows, ...$rowsData->toArray()];
        }

        static::assertCount(3, $rows);
    }

    public function test_cursor_loop_with_exact_fetch_size_multiple_does_extra_fetch(): void
    {
        $client = $this->createClientMock();

        $cursor1 = $this->createCursorMock(rows: [
            ['id' => 1, 'name' => 'User 1'],
            ['id' => 2, 'name' => 'User 2'],
        ], count: 2);

        $cursor2 = $this->createCursorMock(rows: [
            ['id' => 3, 'name' => 'User 3'],
            ['id' => 4, 'name' => 'User 4'],
        ], count: 2);

        $cursor3 = $this->createCursorMock(rows: [], count: 0);

        $client->expects(self::once())->method('getTransactionNestingLevel')->willReturn(1);

        $client->expects(self::exactly(2))->method('execute');

        $client
            ->expects(self::exactly(3))
            ->method('cursor')
            ->willReturnOnConsecutiveCalls($cursor1, $cursor2, $cursor3);

        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');
        $extractor = $extractor->withFetchSize(2);

        $rows = [];

        foreach ($extractor->extract($this->createFlowContext()) as $rowsData) {
            $rows = [...$rows, ...$rowsData->toArray()];
        }

        static::assertCount(4, $rows);
    }

    public function test_schema_is_refused_when_it_was_not_declared(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot describe what it will produce before producing it');

        (new PostgreSqlCursorExtractor($this->createClientStub(), 'SELECT * FROM users'))->schema();
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            (new PostgreSqlCursorExtractor($this->createClientStub(), 'SELECT * FROM users'))
                ->withSchema(schema(int_schema('id')))
                ->schema(),
        );
    }

    public function test_with_fetch_size_returns_self(): void
    {
        $client = $this->createClientStub();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $result = $extractor->withFetchSize(500);

        static::assertSame($extractor, $result);
    }

    public function test_with_fetch_size_validates_positive_value(): void
    {
        $client = $this->createClientStub();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Fetch size must be greater than 0, got 0');

        $extractor->withFetchSize(0);
    }

    public function test_with_fetch_size_validates_positive_value_negative(): void
    {
        $client = $this->createClientStub();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Fetch size must be greater than 0, got -10');

        $extractor->withFetchSize(-10);
    }

    public function test_with_maximum_returns_self(): void
    {
        $client = $this->createClientStub();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $result = $extractor->withMaximum(100);

        static::assertSame($extractor, $result);
    }

    public function test_with_maximum_validates_positive_value(): void
    {
        $client = $this->createClientStub();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got 0');

        $extractor->withMaximum(0);
    }

    public function test_with_maximum_validates_positive_value_negative(): void
    {
        $client = $this->createClientStub();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -5');

        $extractor->withMaximum(-5);
    }

    public function test_with_schema_returns_self(): void
    {
        $client = $this->createClientStub();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $schema = new Schema();
        $result = $extractor->withSchema($schema);

        static::assertSame($extractor, $result);
    }

    /**
     * @return Client&MockObject
     */
    private function createClientMock(): Client
    {
        return $this->createMock(Client::class);
    }

    private function createClientStub(): Client
    {
        return $this->createStub(Client::class);
    }

    /**
     * @param array<array<string, mixed>> $rows
     *
     * @return Cursor&MockObject
     */
    private function createCursorMock(array $rows, int $count): Cursor
    {
        $cursor = $this->createMock(Cursor::class);

        $cursor->expects(self::once())->method('count')->willReturn($count);

        $cursor
            ->method('iterate')
            ->willReturnCallback(static function () use ($rows): Generator {
                foreach ($rows as $row) {
                    yield $row;
                }
            });

        $cursor->expects(self::once())->method('free');

        return $cursor;
    }

    private function createFlowContext(): FlowContext
    {
        return new FlowContext(Config::default());
    }
}
