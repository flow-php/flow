<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\ETL\DSL\{row, rows, schema, str_entry, str_schema};
use Flow\ETL\Adapter\Parquet\RowsNormalizer;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use PHPUnit\Framework\TestCase;

final class RowsNormalizerTest extends TestCase
{
    public function test_casting_error() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage("Can't cast \"NULL\" into \"string\" type");

        $rows = rows(row(str_entry('id', null)));
        $schema = schema(str_schema('id', nullable: false));

        (new RowsNormalizer(Caster::default()))->normalize($rows, $schema);
    }
}
