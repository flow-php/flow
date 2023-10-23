<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DataFrame;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{data_frame: ?DataFrame, prefix: string, rows: ?Rows}>
 */
final class CrossJoinRowsTransformer implements Transformer
{
    private ?Rows $rows = null;

    public function __construct(
        private readonly DataFrame $dataFrame,
        private readonly string $prefix = ''
    ) {
    }

    public function __serialize() : array
    {
        return [
            'data_frame' => null,
            'rows' => $this->rows(),
            'prefix' => $this->prefix,
        ];
    }

    public function __unserialize(array $data) : void
    {
        /** @var Rows $rows */
        $rows = $data['rows'];
        $this->dataFrame = (new Flow())->process($rows);
        $this->prefix = $data['prefix'];
        $this->rows = null;
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->joinCross($this->rows(), $this->prefix);
    }

    private function rows() : Rows
    {
        if ($this->rows === null) {
            $this->rows = $this->dataFrame->fetch();
        }

        return $this->rows;
    }
}
