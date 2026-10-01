<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Comparator;

use PHPUnit\Runner\Extension\Extension;
use PHPUnit\Runner\Extension\Facade;
use PHPUnit\Runner\Extension\ParameterCollection;
use PHPUnit\TextUI\Configuration\Configuration;
use SebastianBergmann\Comparator\Factory;

final class ComparatorExtension implements Extension
{
    public function bootstrap(Configuration $configuration, Facade $facade, ParameterCollection $parameters): void
    {
        Factory::getInstance()->register(new RowsComparator());
        Factory::getInstance()->register(new ColumnComparator());
    }
}
