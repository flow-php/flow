<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{entries: array<string, string>, prefix: string}>
 *
 * @psalm-immutable
 */
final class Condition implements Serializable
{
    /**
     * @var array<string, string>
     */
    private array $entries;

    private string $joinPrefix;

    /**
     * @param array<string, string> $entries
     * @param string $joinPrefix
     */
    private function __construct(array $entries, string $joinPrefix = '')
    {
        $this->entries = $entries;
        $this->joinPrefix = $joinPrefix;
    }

    /**
     * @param array<string, string> $entries
     * @param string $joinPrefix
     *
     * @return self
     */
    public static function on(array $entries, string $joinPrefix = '') : self
    {
        return new self($entries, $joinPrefix);
    }

    public function __serialize() : array
    {
        return [
            'entries' => $this->entries,
            'prefix' => $this->joinPrefix,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entries = $data['entries'];
        $this->joinPrefix = $data['prefix'];
    }

    /**
     * @return array<string>
     */
    public function left() : array
    {
        return \array_keys($this->entries);
    }

    public function meet(Row $left, Row $right) : bool
    {
        foreach ($this->entries as $leftEntry => $rightEntry) {
            try {
                if ($left->valueOf($leftEntry) !== $right->valueOf($rightEntry)) {
                    return false;
                }
            } catch (InvalidArgumentException $e) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return string
     */
    public function prefix() : string
    {
        return $this->joinPrefix;
    }

    /**
     * @return array<string>
     */
    public function right() : array
    {
        return \array_values($this->entries);
    }
}
