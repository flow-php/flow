<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\RowConverter;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{row_casts: array<RowConverter>}>
 */
final class CastTransformer implements Transformer
{
    /**
     * @var RowConverter[]
     */
    private array $rowCasts;

    public function __construct(RowConverter ...$rowCasts)
    {
        $this->rowCasts = $rowCasts;
    }

    public function __serialize() : array
    {
        return [
            'row_casts' => $this->rowCasts,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->rowCasts = $data['row_casts'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            foreach ($this->rowCasts as $caster) {
                $row = $caster->convert($row);
            }

            return $row;
        };

        return $rows->map($transformer);
    }
}
