<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests;

use CmsIg\Seal\Schema\Field;
use CmsIg\Seal\Schema\Index;
use CmsIg\Seal\Schema\Schema;
use Flow\ETL\Adapter\Seal\Tests\Context\SealContext;
use Flow\ETL\Tests\FlowTestCase;

abstract class SealTestCase extends FlowTestCase
{
    protected const INDEX_NAME = 'test';

    private ?SealContext $sealContext = null;

    protected function tearDown(): void
    {
        $this->sealContext?->dropIndex();
        $this->sealContext = null;
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

    protected function sealContext(): SealContext
    {
        if ($this->sealContext === null) {
            $this->sealContext = new SealContext($this->schema(), self::INDEX_NAME);
        }

        return $this->sealContext;
    }
}
