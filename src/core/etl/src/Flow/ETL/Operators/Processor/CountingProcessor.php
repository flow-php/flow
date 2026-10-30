<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Generator;
use SensitiveParameter;

final readonly class CountingProcessor implements Processor
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, self::schema());
    }

    public function breaks(): bool
    {
        return true;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        $count = 0;

        foreach ($rows as $batch) {
            $count += $batch->count();
        }

        yield self::rows($count, $backend);
    }

    /**
     * The one row a count hands out, whether it was counted or known upfront.
     */
    public static function rows(int $count, Backend $backend): Rows
    {
        return (new RowsBuilder(self::schema(), $backend))->appendRecords([['count' => $count]])->finish();
    }

    public static function schema(): Schema
    {
        return new Schema(new IntegerDefinition('count'));
    }
}
