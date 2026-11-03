<?php

declare(strict_types=1);

namespace Flow\ETL\API;

use NoDiscard;

use function array_values;

final readonly class Transformations implements Transformation
{
    /**
     * @var array<int, Transformation>
     */
    public array $transformations;

    public function __construct(Transformation ...$transformations)
    {
        $this->transformations = array_values($transformations);
    }

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        foreach ($this->transformations as $transformation) {
            $dataFrame = $transformation($dataFrame);
        }

        return $dataFrame;
    }
}
