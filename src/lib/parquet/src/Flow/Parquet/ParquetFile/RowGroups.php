<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

final class RowGroups
{
    /**
     * @param array<RowGroup> $rowGroups
     */
    public function __construct(private array $rowGroups)
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

    public function add(RowGroup $rowGroup) : void
    {
        $this->rowGroups[] = $rowGroup;
    }

    /**
     * @return array<RowGroup>
     */
    public function all() : array
    {
        return $this->rowGroups;
    }

    public function rowsCount() : int
    {
        $rowsCount = 0;

        foreach ($this->rowGroups as $rowGroup) {
            $rowsCount += $rowGroup->rowsCount();
        }

        return $rowsCount;
    }

    public function toThrift() : array
    {
        $groups = [];

        foreach ($this->rowGroups as $rowGroup) {
            $groups[] = $rowGroup->toThrift();
        }

        return $groups;
    }
}
