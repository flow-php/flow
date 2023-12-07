<?php declare(strict_types=1);

namespace Flow\RDSL;

use Flow\RDSL\Exception\InvalidArgumentException;

final class Executables
{
    /**
     * @param array<DSLFunction> $executables
     */
    public function __construct(private readonly array $executables)
    {
        foreach ($executables as $executable) {
            if (!$executable instanceof DSLFunction) {
                throw new InvalidArgumentException(\sprintf('Expected instance of "%s", got "%s"', DSLFunction::class, $executable::class));
            }
        }
    }

    /**
     * @return array<DSLFunction>
     */
    public function toArray() : array
    {
        return $this->executables;
    }
}
