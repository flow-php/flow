<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Describe;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\Types\Exception\InvalidTypeException;
use ReflectionClass;

use function class_exists;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function interface_exists;
use function is_a;

final readonly class Rebuild
{
    public function __construct(
        private ProgressStages $progress = new VoidProgress(),
    ) {}

    /**
     * Every entry of $normalized rebuilt with of(), keys kept.
     *
     * @template T of object
     *
     * @param class-string<T> $type
     *
     * @throws InvalidArgumentException
     *
     * @return array<array-key, T>
     */
    public function all(mixed $normalized, string $type): array
    {
        $objects = [];

        // @mago-ignore analysis:mixed-assignment
        foreach (type_array()->assert($normalized) as $key => $item) {
            $objects[$key] = $this->of($item, $type);
        }

        return $objects;
    }

    /**
     * The object of a normalized description (the shape Description::normalize() writes).
     *
     * @template T of object
     *
     * @param class-string<T> $type
     *
     * @throws InvalidArgumentException
     *
     * @return T
     */
    public function of(mixed $normalized, string $type): object
    {
        return $this->object(Description::fromArray(type_array()->assert($normalized)), $type);
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $type
     *
     * @throws InvalidArgumentException
     *
     * @return T
     */
    public function object(Description $description, string $type): object
    {
        $class = $description->class;

        if (!class_exists($class) && !interface_exists($class)) {
            throw InvalidArgumentException::because(
                'Class %s is not autoloadable here: install the package that provides it',
                $class,
            );
        }

        if ((new ReflectionClass($class))->isAbstract()) {
            throw InvalidArgumentException::because('%s is not a concrete class', $class);
        }

        if (!is_a($class, Describable::class, true)) {
            throw InvalidArgumentException::because('%s is not Describable', $class);
        }

        // @mago-ignore analysis:possibly-static-access-on-interface
        $built = $class::fromDescription($description->data, $this);

        if (!$built instanceof $type) {
            throw InvalidArgumentException::because('%s is not a %s', $class, $type);
        }

        return $built;
    }

    /**
     * @throws InvalidArgumentException
     * @throws InvalidTypeException
     */
    public function progress(StageName $stage, mixed $described = null): Progress
    {
        if ($described === null) {
            return $this->progress->stage($stage);
        }

        $description = Description::fromArray(type_array()->assert($described));

        // Foundation does not know the runtime's stage class: every description but VoidProgress's is a stage
        return $description->class === VoidProgress::class
            ? $this->progress->stage($stage)
            : $this->progress->stage(
                StageName::from(type_string()->assert($description->data['name'] ?? null)),
                ProgressUnit::rows,
                type_integer()->assert($description->data['id'] ?? null),
                type_union(type_string(), type_null())->assert($description->data['subject'] ?? null),
            );
    }
}
