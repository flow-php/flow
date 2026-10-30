<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

/**
 * Closes a cycle only on the second batch, so the re-entry lands after the pipeline has already
 * yielded once. A cycle closing on the first batch is caught before Pipeline::process() ever parks.
 */
final class ReadsBackOnSecondBatch implements Transformer
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public int $transforms = 0;

    private ?DataFrame $dataFrame = null;

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function readBackFrom(DataFrame $dataFrame): void
    {
        $this->dataFrame = $dataFrame;
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $this->transforms++;

        if ($this->transforms === 2 && $this->dataFrame !== null) {
            $this->dataFrame->fetch();
        }

        return $rows;
    }
}
