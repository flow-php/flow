<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Unit;

use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\HashIdFactory;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class HashIdFactoryTest extends TestCase
{
    public function test_create_row() : void
    {
        $factory = new HashIdFactory('first_name', 'last_name');

        $this->assertEquals(
            new Row\Entry\StringEntry(
                'id',
                \hash('xxh128', 'John:Doe')
            ),
            $factory->create(
                Row::create(Entry::string('first_name', 'John'), Entry::string('last_name', 'Doe'))
            )
        );
    }

    public function test_create_row_with_different_hash() : void
    {
        $factory = (new HashIdFactory('first_name', 'last_name'))->withAlgorithm('sha1');

        $this->assertEquals(
            new Row\Entry\StringEntry(
                'id',
                \sha1('John:Doe')
            ),
            $factory->create(
                Row::create(Entry::string('first_name', 'John'), Entry::string('last_name', 'Doe'))
            )
        );
    }

    public function test_invalid_hash_algorithm_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported hash algorithm name provided: whatever, did you mean: ');

        (new HashIdFactory('first_name'))->withAlgorithm('whatever');
    }
}
