<?php

declare(strict_types=1);

namespace Flow\Types\Value;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\RuntimeException;
use InvalidArgumentException as BaseInvalidArgumentException;
use Ramsey\Uuid\Uuid as RamseyUuid;
use Ramsey\Uuid\UuidInterface;
use Stringable;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

use function class_exists;
use function is_string;
use function preg_match;
use function strlen;

final readonly class Uuid implements Stringable
{
    /**
     * This regexp is a port of the Uuid library,
     * which is copyright Ben Ramsey, @see https://github.com/ramsey/uuid.
     */
    private const string UUID_REGEXP = '/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/ms';

    private string $value;

    /**
     * @throws InvalidArgumentException|RuntimeException
     */
    public function __construct(string|UuidInterface|SymfonyUuid $value)
    {
        if (is_string($value)) {
            try {
                if (class_exists(RamseyUuid::class)) {
                    $this->value = (string) RamseyUuid::fromString($value);
                } elseif (class_exists(SymfonyUuid::class)) {
                    $this->value = SymfonyUuid::fromString($value)->toRfc4122();
                } elseif (self::isValid($value)) {
                    $this->value = $value;
                } else {
                    throw new RuntimeException(
                        "RamseyUuid nor SymfonyUuid class not found, please add 'ramsey/uuid' or 'symfony/uid' as a dependency to the project first.",
                    );
                }
            } catch (BaseInvalidArgumentException $e) {
                throw new InvalidArgumentException("Invalid UUID: '{$value}'", (int) $e->getCode(), $e);
            }
        } elseif ($value instanceof UuidInterface) {
            $this->value = $value->toString();
        } else {
            $this->value = $value->toRfc4122();
        }
    }

    public static function fromString(string $value): self
    {
        return new self($value);
    }

    public static function isValid(string $value): bool
    {
        if (strlen($value) !== 36) {
            return false;
        }

        return 1 === preg_match(self::UUID_REGEXP, $value);
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function isEqual(self $type): bool
    {
        return $this->toString() === $type->toString();
    }

    public function toString(): string
    {
        return $this->value;
    }
}
