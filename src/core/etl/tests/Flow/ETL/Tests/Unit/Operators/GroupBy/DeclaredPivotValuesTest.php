<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\GroupBy\DeclaredPivotValues;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DeclaredPivotValuesTest extends FlowTestCase
{
    public function test_duplicate_values_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pivot values must be unique');

        new DeclaredPivotValues('USA', 'China', 'USA');
    }

    public function test_it_keeps_the_declaration_order(): void
    {
        static::assertSame(['USA', 0, 'China'], (new DeclaredPivotValues('USA', 0, 'China'))->all());
    }

    public function test_no_values_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pivot requires at least one value');

        new DeclaredPivotValues();
    }

    public function test_resolving_a_declared_form_reads_nothing(): void
    {
        $values = new DeclaredPivotValues('USA');
        static::assertSame($values, $values->resolve(
            df()->read($extractor = new CountingExtractor(schema(str_schema('country'))))->get(),
            ref('country'),
        ));
        static::assertSame(0, $extractor->extractCalls);
    }
}
