<?php

declare(strict_types=1);

namespace Flow\ETL\Join\Comparison;

use Flow\ETL\Join\Comparison;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;

/**
 * @implements Comparison<array{entry_left: EntryReference, entry_right: EntryReference}>
 */
final class Equal implements Comparison
{
    private readonly EntryReference $entryLeft;
    private readonly EntryReference $entryRight;

    public function __construct(
        string|EntryReference $entryLeft,
        string|EntryReference $entryRight
    ) {
        $this->entryLeft = EntryReference::init($entryLeft);
        $this->entryRight = EntryReference::init($entryRight);
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
     * @return array<EntryReference>
     */
    public function left() : array
    {
        return [$this->entryLeft];
    }

    /**
     * @return array<EntryReference>
     */
    public function right() : array
    {
        return [$this->entryRight];
    }
}
