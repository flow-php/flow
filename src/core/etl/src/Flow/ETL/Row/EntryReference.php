<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

/**
 * @implements Reference<array{entry: string, alias: ?string}>
 */
final class EntryReference implements Reference
{
    private ?string $alias = null;

    public function __construct(private readonly string $entry)
    {
    }

    public static function init(string|self $ref) : self
    {
        if (\is_string($ref)) {
            return new self($ref);
        }

        return $ref;
    }

    /**
     * @param Reference|string ...$names
     *
     * @return array<EntryReference>
     */
    public static function initAll(string|Reference ...$names) : array
    {
        $refs = [];

        foreach ($names as $name) {
            if ($name instanceof StructureReference) {
                $refs = \array_merge($refs, $name->to());
            } else {
                /**
                 * @psalm-suppress PossiblyInvalidArgument
                 *
                 * @phpstan-ignore-next-line
                 */
                $refs[] = self::init($name);
            }
        }

        return $refs;
    }

    public function __serialize() : array
    {
        return [
            'entry' => $this->entry,
            'alias' => $this->alias,
        ];
    }

    public function __toString() : string
    {
        return $this->name();
    }

    public function __unserialize(array $data) : void
    {
        $this->entry = $data['entry'];
        $this->alias = $data['alias'];
    }

    public function as(string $alias) : self
    {
        $this->alias = $alias;

        return $this;
    }

    public function hasAlias() : bool
    {
        return $this->alias !== null;
    }

    public function is(Reference $ref) : bool
    {
        return $this->name() === $ref->name();
    }

    public function name() : string
    {
        return $this->alias ?? $this->entry;
    }

    public function to() : string
    {
        return $this->entry;
    }
}
