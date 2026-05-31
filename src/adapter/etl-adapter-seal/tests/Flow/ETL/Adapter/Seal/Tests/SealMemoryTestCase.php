<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests;

use CmsIg\Seal\Adapter\Memory\MemoryAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Field;
use CmsIg\Seal\Schema\Index;
use CmsIg\Seal\Schema\Schema;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Seal\seal_create_index;
use function Flow\ETL\Adapter\Seal\seal_drop_index;

abstract class SealMemoryTestCase extends FlowTestCase
{
    protected const INDEX_NAME = 'test';

    protected EngineInterface $engine;

    protected function setUp(): void
    {
        $this->engine = new Engine(new MemoryAdapter(), $this->schema());

        seal_create_index($this->engine, self::INDEX_NAME);
    }

    protected function tearDown(): void
    {
        seal_drop_index($this->engine, self::INDEX_NAME);
    }

    protected function schema(): Schema
    {
        return new Schema([
            self::INDEX_NAME => new Index(self::INDEX_NAME, [
                'id' => new Field\IdentifierField('id'),
                'name' => new Field\TextField('name'),
                'age' => new Field\IntegerField('age', filterable: true, sortable: true),
            ]),
        ]);
    }
}
