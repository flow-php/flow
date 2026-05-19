<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\RowMapper;

use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\RowMapper;
use ReflectionClass;
use Throwable;

use function array_key_exists;
use function array_values;
use function class_exists;
use function Flow\Types\DSL\type_instance_of;

/**
 * Maps database rows directly to constructor parameters.
 *
 * Requirements:
 * - SQL column names must match constructor parameter names exactly (1:1)
 * - Class must have a constructor with all parameters
 * - Use SQL aliases if column names differ from parameter names:
 *   SELECT created_at AS createdAt FROM users
 *
 * @template T of object
 *
 * @implements RowMapper<T>
 */
final readonly class ConstructorMapper implements RowMapper
{
    /** @var list<\ReflectionParameter> */
    private array $parameters;

    /** @var \ReflectionClass<object> */
    private ReflectionClass $reflection;

    /**
     * @param class-string<T> $class
     *
     * @throws MappingException
     */
    public function __construct(
        private string $class,
    ) {
        if (!class_exists($this->class)) {
            throw MappingException::mappingFailed($this->class, 'Class does not exist');
        }

        $this->reflection = new ReflectionClass($this->class);

        $constructor = $this->reflection->getConstructor();

        if ($constructor === null) {
            throw MappingException::mappingFailed($this->class, 'Class has no constructor');
        }

        $this->parameters = array_values($constructor->getParameters());
    }

    /**
     * @return T
     */
    public function map(array $row, Context $context): object
    {
        $args = [];

        foreach ($this->parameters as $param) {
            $paramName = $param->getName();

            if (array_key_exists($paramName, $row)) {
                $args[$paramName] = $row[$paramName];
            } elseif ($param->isDefaultValueAvailable()) {
                continue;
            } elseif ($param->allowsNull()) {
                $args[$paramName] = null;
            } else {
                throw MappingException::propertyNotFound($this->class, $paramName);
            }
        }

        try {
            return type_instance_of($this->class)->assert($this->reflection->newInstanceArgs($args));
        } catch (Throwable $e) {
            throw MappingException::mappingFailed($this->class, $e->getMessage());
        }
    }
}
