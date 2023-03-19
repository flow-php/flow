<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter;

/**
 * @implements Filter<array{callback: callable(Row $row) : bool}>
 */
final class Callback implements Filter
{
    /**
     * @var callable(Row) : bool
     */
    private $callback;

    /**
     * @param callable(Row $row) : bool $callback
     */
    public function __construct(callable $callback)
    {
        $this->callback = $callback;
    }

    public function __serialize() : array
    {
        return [
            'callback' => $this->callback,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->callback = $data['callback'];
    }

    public function keep(Row $row) : bool
    {
        return ($this->callback)($row);
    }
}
