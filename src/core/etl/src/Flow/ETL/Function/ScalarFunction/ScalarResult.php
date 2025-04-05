<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ScalarFunction;

use Flow\ETL\PHP\Type\{Type, TypeDetector};

final readonly class ScalarResult
{
    /**
     * @var Type<mixed>
     */
    public Type $type;

    /**
     * @param mixed $value
     * @param Type<mixed> $type
     */
    public function __construct(
        public mixed $value,
        Type $type,
    ) {
        if ($value === null) {
            $this->type = $type->makeNullable(true);
        } else {
            $this->type = $type;
        }
    }

    public static function from(mixed $value) : self
    {
        return new self($value, (new TypeDetector())->detectType($value));
    }
}
