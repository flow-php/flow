<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Pagination\Key;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
use Flow\ETL\Adapter\PostgreSql\Pagination\Order;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlKeySetExtractor;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Client;
use PHPUnit\Framework\MockObject\Stub;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class PostgreSqlKeySetExtractorTest extends FlowTestCase
{
    public function test_schema_is_refused_when_it_was_not_declared(): void
    {
        $extractor = new PostgreSqlKeySetExtractor(
            $this->createClientMock(),
            'SELECT * FROM users ORDER BY id',
            new KeySet(new Key('id', Order::ASC)),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot describe what it will produce before producing it');

        $extractor->schema();
    }

    public function test_schema_is_the_declared_one(): void
    {
        $extractor = new PostgreSqlKeySetExtractor(
            $this->createClientMock(),
            'SELECT * FROM users ORDER BY id',
            new KeySet(new Key('id', Order::ASC)),
        );

        static::assertEquals(schema(int_schema('id')), $extractor->withSchema(schema(int_schema('id')))->schema());
    }

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
     * @return Client&Stub
     */
    private function createClientMock(): Client
    {
        return $this->createStub(Client::class);
    }
}
