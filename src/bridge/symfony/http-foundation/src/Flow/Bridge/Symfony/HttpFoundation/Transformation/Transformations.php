<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Transformation;

use Flow\ETL\{DataFrame, Transformation};

final readonly class Transformations implements Transformation
{
    /**
     * @param Transformation ...$transformations
     */
    private array $transformations;

    public function __construct(Transformation ...$transformations)
    {
        $this->transformations = $transformations;
    }

    public function transform(DataFrame $dataFrame) : DataFrame
    {
        foreach ($this->transformations as $transformation) {
            $dataFrame = $transformation->transform($dataFrame);
        }

        return $dataFrame;
    }
}
