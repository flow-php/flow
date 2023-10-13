<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

final class RowGroups
{
    /**
     * @param array<RowGroup> $rowGroups
     */
    public function __construct(private readonly array $rowGroups)
    {
    }

    /**
     * @param array<\Flow\Parquet\Thrift\RowGroup> $rowGroups
     */
    public static function fromThrift(array $rowGroups) : self
    {
        $groups = [];

        foreach ($rowGroups as $rowGroup) {
            $groups[] = RowGroup::fromThrift($rowGroup);
        }

        return new self($groups);
    }

    /**
     * @return array<RowGroup>
     */
    public function all() : array
    {
        return $this->rowGroups;
    }
}
