<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;

/**
 * @implements Comparison<array{entry_left: string, entry_right: string}>
 *
 * @psalm-immutable
 */
final class Equal implements Comparison
{
    public function __construct(private readonly string $entryLeft, private readonly string $entryRight)
    {
    }

    public function __serialize() : array
    {
        return [
            'entry_left' => $this->entryLeft,
            'entry_right' => $this->entryRight,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryLeft = $data['entry_left'];
        $this->entryRight = $data['entry_right'];
    }

    public function compare(Row $left, Row $right) : bool
    {
        return $left->valueOf($this->entryLeft) == $right->valueOf($this->entryRight);
    }

    /**
     * @return array<string>
     */
    public function left() : array
    {
        return [$this->entryLeft];
    }

    /**
     * @return array<string>
     */
    public function right() : array
    {
        return [$this->entryRight];
    }
}
