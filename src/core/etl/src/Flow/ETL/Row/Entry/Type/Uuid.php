<?php declare(strict_types=1);

namespace Flow\ETL\Row\Entry\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;

final class Uuid
{
    /**
     * This regexp is a port of the Uuid library,
     * which is copyright Ben Ramsey, @see https://github.com/ramsey/uuid.
     */
    public const UUID_REGEXP = '/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/ms';

    private readonly string $value;

    /**
     * @throws InvalidArgumentException|RuntimeException
     */
    public function __construct(string|\Ramsey\Uuid\UuidInterface|\Symfony\Component\Uid\Uuid $value)
    {
        if (\is_string($value)) {
            try {
                if (\class_exists(\Ramsey\Uuid\UuidInterface::class)) {
                    $this->value = (string) \Ramsey\Uuid\Uuid::fromString($value);
                } elseif (\class_exists(\Symfony\Component\Uid\Uuid::class)) {
                    $this->value = \Symfony\Component\Uid\Uuid::fromString($value)->toRfc4122();
                } else {
                    throw new RuntimeException("\Ramsey\Uuid\Uuid nor \Symfony\Component\Uid\Uuid class not found, please add 'ramsey/uuid' or 'symfony/uid' as a dependency to the project first.");
                }
            } catch (\InvalidArgumentException $e) {
                throw new InvalidArgumentException("Invalid UUID: '{$value}'", 0, $e);
            }
        } elseif ($value instanceof \Ramsey\Uuid\UuidInterface) {
            $this->value = $value->toString();
        } else {
            $this->value = $value->toRfc4122();
        }
    }

    public static function fromString(string $value) : self
    {
        return new self($value);
    }

    public function isEqual(self $type) : bool
    {
        return $this->toString() === $type->toString();
    }

    public function toString() : string
    {
        return $this->value;
    }
}
