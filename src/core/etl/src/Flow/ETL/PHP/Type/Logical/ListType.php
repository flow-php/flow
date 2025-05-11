<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\ETL\PHP\Type\{Type, TypeFactory};

/**
 * @implements Type<list<mixed>>
 */
final readonly class ListType implements Type
{
    /**
     * @param Type<mixed> $element
     */
    public function __construct(private Type $element)
    {
    }

    public static function fromArray(array $data) : self
    {
        return new self(TypeFactory::fromArray($data['element']), $data['nullable'] ?? false);
    }

    public function assert(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : array
    {
        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            if (!\is_array($value)) {
                return [$this->element()->cast($value)];
            }

            $castedList = [];

            foreach ($value as $key => $item) {
                $castedList[$key] = $this->element()->cast($item);
            }

            return $castedList;
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    /**
     * @return Type<mixed>
     */
    public function element() : Type
    {
        return $this->element;
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {

            return false;
        }

        if ([] !== $value && !\array_is_list($value)) {
            return false;
        }

        foreach ($value as $item) {
            if (!$this->element->isValid($item)) {
                return false;
            }
        }

        return true;
    }

    public function normalize() : array
    {
        return [
            'type' => 'list',
            'element' => $this->element->normalize(),
        ];
    }

    public function toString() : string
    {
        return 'list<' . $this->element->toString() . '>';
    }
}
