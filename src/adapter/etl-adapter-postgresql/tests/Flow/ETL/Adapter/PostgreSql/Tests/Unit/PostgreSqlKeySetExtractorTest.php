<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Pagination\Key;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
use Flow\ETL\Adapter\PostgreSql\Pagination\Order;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlKeySetExtractor;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\MockObject\MockObject;

final class PostgreSqlKeySetExtractorTest extends FlowTestCase
{
    public function test_with_maximum_validates_positive_value(): void
    {
        $client = $this->createClientMock();
        $keySet = new KeySet(new Key('id', Order::ASC));
        $extractor = new PostgreSqlKeySetExtractor($client, 'SELECT * FROM users ORDER BY id', $keySet);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got 0');

        $extractor->withMaximum(0);
    }

    public function test_with_maximum_validates_positive_value_negative(): void
    {
        $client = $this->createClientMock();
        $keySet = new KeySet(new Key('id', Order::ASC));
        $extractor = new PostgreSqlKeySetExtractor($client, 'SELECT * FROM users ORDER BY id', $keySet);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -5');

        $extractor->withMaximum(-5);
    }

    public function test_with_page_size_validates_positive_value(): void
    {
        $client = $this->createClientMock();
        $keySet = new KeySet(new Key('id', Order::ASC));
        $extractor = new PostgreSqlKeySetExtractor($client, 'SELECT * FROM users ORDER BY id', $keySet);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Page size must be greater than 0, got 0');

        $extractor->withPageSize(0);
    }

    public function test_with_page_size_validates_positive_value_negative(): void
    {
        $client = $this->createClientMock();
        $keySet = new KeySet(new Key('id', Order::ASC));
        $extractor = new PostgreSqlKeySetExtractor($client, 'SELECT * FROM users ORDER BY id', $keySet);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Page size must be greater than 0, got -10');

        $extractor->withPageSize(-10);
    }

    public function test_with_schema_returns_self(): void
    {
        $client = $this->createClientMock();
        $keySet = new KeySet(new Key('id', Order::ASC));
        $extractor = new PostgreSqlKeySetExtractor($client, 'SELECT * FROM users ORDER BY id', $keySet);

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
}
