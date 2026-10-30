<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\SchemaValidationException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\SchemaValidator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;

final readonly class SchemaValidationLoader implements Loader
{
    public function __construct(
        private Schema $expected,
        private SchemaValidator $validator,
    ) {}

    public function load(Rows $rows, Backend $backend): void
    {
        $validation = $this->validator->validate($this->expected, $rows->schema());

        if (!$validation->isValid()) {
            throw new SchemaValidationException($validation->toString());
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            Schema::fromArray(type_array()->assert($data['expected'] ?? null)),
            $rebuild->of($data['validator'] ?? null, SchemaValidator::class),
        );
    }

    public function describe(): ?Description
    {
        return (
            $this->validator instanceof Describable
                ? Description::compose(self::class, [
                    'expected' => $this->expected->normalize(),
                    'validator' => $this->validator,
                ])
                : null
        );
    }
}
