<?php declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class UntilTransformer implements Transformer
{
    private bool $limitReached = false;

    public function __construct(private readonly ScalarFunction $function)
    {
    }

    public function __serialize() : array
    {
        return [
            'function' => $this->function,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->function = $data['function'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        if ($this->limitReached) {
            throw new LimitReachedException(0);
        }

        $nextRows = [];

        foreach ($rows as $row) {
            if (!$this->function->eval($row)) {
                $this->limitReached = true;
            } else {
                $nextRows[] = $row;
            }
        }

        return new Rows(...$nextRows);
    }
}
