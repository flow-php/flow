<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Schema\Definition;

final class ColumnMother
{
    /**
     * @param Definition<mixed> $definition
     * @param list<mixed> $values
     */
    public static function of(Definition $definition, array $values): Column
    {
        $builder = (new PhpBackend())->builder($definition);
        $builder->appendMany($values);

        return $builder->finish();
    }
}
