<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlCursorExtractor;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\MockObject\MockObject;

final class PostgreSqlCursorExtractorTest extends FlowTestCase
{
    public function test_with_fetch_size_returns_self() : void
    {
        $client = $this->createClientMock();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $result = $extractor->withFetchSize(500);

        self::assertSame($extractor, $result);
    }

    public function test_with_fetch_size_validates_positive_value() : void
    {
        $client = $this->createClientMock();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Fetch size must be greater than 0, got 0');

        $extractor->withFetchSize(0);
    }

    public function test_with_fetch_size_validates_positive_value_negative() : void
    {
        $client = $this->createClientMock();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Fetch size must be greater than 0, got -10');

        $extractor->withFetchSize(-10);
    }

    public function test_with_maximum_returns_self() : void
    {
        $client = $this->createClientMock();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $result = $extractor->withMaximum(100);

        self::assertSame($extractor, $result);
    }

    public function test_with_maximum_validates_positive_value() : void
    {
        $client = $this->createClientMock();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got 0');

        $extractor->withMaximum(0);
    }

    public function test_with_maximum_validates_positive_value_negative() : void
    {
        $client = $this->createClientMock();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -5');

        $extractor->withMaximum(-5);
    }

    public function test_with_schema_returns_self() : void
    {
        $client = $this->createClientMock();
        $extractor = new PostgreSqlCursorExtractor($client, 'SELECT * FROM users');

        $schema = new Schema();
        $result = $extractor->withSchema($schema);

        self::assertSame($extractor, $result);
    }

    /**
     * @return Client&MockObject
     */
    private function createClientMock() : Client
    {
        return $this->createMock(Client::class);
    }
}
