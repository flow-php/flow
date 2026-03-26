<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Dremel\Validator\DisabledValidator;
use Flow\Parquet\ParquetFile\Data\{Converter, DataConverter};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final readonly class DremelShredder
{
    public function __construct(
        private Validator $validator,
        private DataConverter $dataConverter,
    ) {
    }

    /**
     * @param array<array<string, mixed>> $rows
     *
     * @return array<string, WriteFlatColumnValues> keyed by flatPath
     */
    public function shred(Schema $schema, array $rows) : array
    {
        /** @var array<string, WriteFlatColumnValues> $targets */
        $targets = [];

        /** @var array<string, bool> $columnRequired */
        $columnRequired = [];

        /** @var array<string, ?Converter> $columnConverters */
        $columnConverters = [];

        foreach ($schema->columnsFlat() as $flatColumn) {
            $fp = $flatColumn->flatPath();
            $targets[$fp] = new WriteFlatColumnValues($flatColumn);
            $columnRequired[$fp] = $flatColumn->repetition()?->isRequired() ?? false;
            $columnConverters[$fp] = $this->dataConverter->resolveConverter($flatColumn);
        }

        $shouldValidate = !$this->validator instanceof DisabledValidator;

        /** @var array<FlatPlan|ListPlan|MapPlan|StructPlan> $plans */
        $plans = [];

        foreach ($schema->columns() as $column) {
            $plans[] = $this->buildPlan($column, $targets, $columnRequired, $columnConverters);
        }

        foreach ($plans as $plan) {
            foreach ($rows as $row) {
                $value = $row[$plan->childName] ?? null;

                if ($shouldValidate) {
                    $this->validator->validate($schema->get($plan->childName), $value);
                }

                /** @var array<string, bool> $rowFirstWrite */
                $rowFirstWrite = [];

                if ($plan instanceof FlatPlan) {
                    $target = $plan->target;
                    $converter = $plan->converter;

                    $defLvl = 0;

                    if (!$plan->isRequired && $value !== null) {
                        $defLvl = 1;
                    }

                    $target->repetitionLevels[] = 0;
                    $target->definitionLevels[] = $defLvl;

                    if ($value !== null) {
                        /** @phpstan-ignore assign.propertyType */
                        $target->values[] = $converter !== null ? $converter->toParquetType($value) : $value;
                    }
                } elseif ($plan instanceof ListPlan) {
                    /** @phpstan-ignore-next-line */
                    $this->execList($plan, $value, 0, 0, 0, $shouldValidate, $rowFirstWrite);
                } elseif ($plan instanceof MapPlan) {
                    /** @phpstan-ignore-next-line */
                    $this->execMap($plan, $value, 0, 0, 0, $shouldValidate, $rowFirstWrite);
                } elseif ($plan instanceof StructPlan) {
                    $this->execStruct($plan, $value, 0, 0, 0, $shouldValidate, $rowFirstWrite);
                }
            }
        }

        return $targets;
    }

    /**
     * @param array<string, WriteFlatColumnValues> $targets
     * @param array<string, bool> $columnRequired
     * @param array<string, ?Converter> $columnConverters
     */
    private function buildPlan(Column $column, array $targets, array $columnRequired, array $columnConverters) : FlatPlan|StructPlan|ListPlan|MapPlan
    {
        if ($column instanceof FlatColumn) {
            $fp = $column->flatPath();

            return new FlatPlan(
                $fp,
                $column->name(),
                $columnRequired[$fp],
                $columnConverters[$fp],
                $targets[$fp],
            );
        }

        /** @var NestedColumn $column */
        $isRequired = $column->repetition()?->isRequired() ?? false;

        if ($column->isList()) {
            $listElement = $column->getListElement();

            return new ListPlan(
                $column->name(),
                $isRequired,
                $this->buildPlan($listElement, $targets, $columnRequired, $columnConverters),
                $listElement instanceof NestedColumn ? $listElement : null,
            );
        }

        if ($column->isMap()) {
            $keyColumn = $column->getMapKeyColumn();
            $valueColumn = $column->getMapValueColumn();
            $keyFp = $keyColumn->flatPath();

            return new MapPlan(
                $column->name(),
                $isRequired,
                new FlatPlan(
                    $keyFp,
                    $keyColumn->name(),
                    $columnRequired[$keyFp],
                    $columnConverters[$keyFp],
                    $targets[$keyFp],
                ),
                $valueColumn !== null ? $this->buildPlan($valueColumn, $targets, $columnRequired, $columnConverters) : null,
                [
                    'flatPath' => $keyFp,
                    'isRequired' => $columnRequired[$keyFp],
                    'converter' => $columnConverters[$keyFp],
                    'target' => $targets[$keyFp],
                ],
                $valueColumn instanceof NestedColumn ? $valueColumn : null,
            );
        }

        $children = [];
        /** @var array<array{flatPath: string, target: WriteFlatColumnValues}> $nullFlatChildren */
        $nullFlatChildren = [];

        foreach ($column->children() as $child) {
            $childPlan = $this->buildPlan($child, $targets, $columnRequired, $columnConverters);
            $children[] = $childPlan;

            if ($childPlan instanceof FlatPlan) {
                $nullFlatChildren[] = [
                    'flatPath' => $childPlan->flatPath,
                    'target' => $childPlan->target,
                ];
            }
        }

        return new StructPlan(
            $column->name(),
            $isRequired,
            $children,
            $nullFlatChildren,
        );
    }

    /**
     * @param null|array<mixed> $listValue
     * @param array<string, bool> $rowFirstWrite
     */
    private function execList(ListPlan $plan, ?array $listValue, int $definitionLevel, int $repetitionLevel, int $depth, bool $shouldValidate, array &$rowFirstWrite) : void
    {
        $repetitionLevel++;
        $depth++;
        $element = $plan->element;

        if ($element instanceof FlatPlan) {
            $fp = $element->flatPath;
            $target = $element->target;
            $isRequired = $element->isRequired;
            $converter = $element->converter;

            if ($listValue === null) {
                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$fp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$fp] = true;
                }

                $target->repetitionLevels[] = $repLvl;
                $target->definitionLevels[] = $definitionLevel;

                return;
            }

            if (!$plan->isRequired) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$fp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$fp] = true;
                }

                $target->repetitionLevels[] = $repLvl;
                $target->definitionLevels[] = $definitionLevel;

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $value) {
                $defLvl = $definitionLevel;
                $repLvl = $i === 0 ? $repetitionLevel - 1 : $depth;

                if (!$isRequired && $value !== null) {
                    $defLvl++;
                }

                if (!isset($rowFirstWrite[$fp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$fp] = true;
                }

                $target->repetitionLevels[] = $repLvl;
                $target->definitionLevels[] = $defLvl;

                if ($value !== null) {
                    /** @phpstan-ignore assign.propertyType */
                    $target->values[] = $converter !== null ? $converter->toParquetType($value) : $value;
                }
            }

            return;
        }

        if ($element instanceof ListPlan) {
            if ($listValue === null) {
                $this->execList($element, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            if (!$plan->isRequired) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                if ($shouldValidate && $plan->elementColumn !== null) {
                    $this->validator->validate($plan->elementColumn, null);
                }
                $this->execList($element, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $value) {
                /** @phpstan-ignore-next-line */
                $this->execList($element, $value, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $depth, $shouldValidate, $rowFirstWrite);
            }

            return;
        }

        if ($element instanceof MapPlan) {
            if ($listValue === null) {
                $this->execMap($element, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            if (!$plan->isRequired) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                if ($shouldValidate && $plan->elementColumn !== null) {
                    $this->validator->validate($plan->elementColumn, null);
                }
                $this->execMap($element, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $mapValue) {
                /** @phpstan-ignore-next-line */
                $this->execMap($element, $mapValue, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $depth, $shouldValidate, $rowFirstWrite);
            }

            return;
        }

        /** @var StructPlan $element */
        if ($listValue === null) {
            $this->execStruct($element, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

            return;
        }

        if (!$plan->isRequired) {
            $definitionLevel++;
        }

        if (!\count($listValue)) {
            if ($shouldValidate && $plan->elementColumn !== null) {
                $this->validator->validate($plan->elementColumn, null);
            }
            $this->execStruct($element, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

            return;
        }

        $definitionLevel++;

        foreach ($listValue as $i => $listElementValue) {
            $this->execStruct($element, $listElementValue, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $depth, $shouldValidate, $rowFirstWrite);
        }
    }

    /**
     * @param null|array<mixed> $mapValue
     * @param array<string, bool> $rowFirstWrite
     */
    private function execMap(MapPlan $plan, ?array $mapValue, int $definitionLevel, int $repetitionLevel, int $depth, bool $shouldValidate, array &$rowFirstWrite) : void
    {
        $repetitionLevel++;
        $depth++;
        $keyPlan = $plan->keyPlan;
        $valuePlan = $plan->valuePlan;

        if ($valuePlan === null) {
            $keyFp = $keyPlan->flatPath;
            $keyTarget = $keyPlan->target;
            $keyConverter = $keyPlan->converter;

            if ($mapValue === null) {
                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$keyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$keyFp] = true;
                }

                $keyTarget->repetitionLevels[] = $repLvl;
                $keyTarget->definitionLevels[] = $definitionLevel;

                return;
            }

            if (!$plan->isRequired) {
                $definitionLevel++;
            }

            $definitionLevel++;
            $index = 0;

            foreach ($mapValue as $key => $value) {
                $repLvl = $index === 0 ? ($repetitionLevel - 1) : $repetitionLevel;

                if (!isset($rowFirstWrite[$keyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$keyFp] = true;
                }

                $defLvl = $definitionLevel + ($keyPlan->isRequired ? 0 : 1);

                $keyTarget->repetitionLevels[] = $repLvl;
                $keyTarget->definitionLevels[] = $defLvl;
                /** @phpstan-ignore assign.propertyType */
                $keyTarget->values[] = $keyConverter !== null ? $keyConverter->toParquetType($key) : $key;
                $index++;
            }

            return;
        }

        if ($valuePlan instanceof FlatPlan) {
            $keyFp = $keyPlan->flatPath;
            $valFp = $valuePlan->flatPath;
            $keyTarget = $keyPlan->target;
            $valTarget = $valuePlan->target;
            $keyRequired = $keyPlan->isRequired;
            $valRequired = $valuePlan->isRequired;
            $keyConverter = $keyPlan->converter;
            $valConverter = $valuePlan->converter;

            if ($mapValue === null) {
                $optKeyFp = $plan->optionalKey['flatPath'];
                $optKeyTarget = $plan->optionalKey['target'];

                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$optKeyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$optKeyFp] = true;
                }

                $optKeyTarget->repetitionLevels[] = $repLvl;
                $optKeyTarget->definitionLevels[] = $definitionLevel;

                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$valFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$valFp] = true;
                }

                $valTarget->repetitionLevels[] = $repLvl;
                $valTarget->definitionLevels[] = $definitionLevel;

                return;
            }

            if (!$plan->isRequired) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                $optKeyFp = $plan->optionalKey['flatPath'];
                $optKeyTarget = $plan->optionalKey['target'];

                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$optKeyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$optKeyFp] = true;
                }

                $optKeyTarget->repetitionLevels[] = $repLvl;
                $optKeyTarget->definitionLevels[] = $definitionLevel;

                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$valFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$valFp] = true;
                }

                $valTarget->repetitionLevels[] = $repLvl;
                $valTarget->definitionLevels[] = $definitionLevel;

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($mapValue as $key => $value) {
                $repLevel = $index === 0 ? $repetitionLevel - 1 : $depth;

                $defLvl = $definitionLevel;
                $repLvl = $repLevel;

                if (!$keyRequired) {
                    $defLvl++;
                }

                if (!isset($rowFirstWrite[$keyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$keyFp] = true;
                }

                $keyTarget->repetitionLevels[] = $repLvl;
                $keyTarget->definitionLevels[] = $defLvl;
                /** @phpstan-ignore assign.propertyType */
                $keyTarget->values[] = $keyConverter !== null ? $keyConverter->toParquetType($key) : $key;

                $defLvl = $definitionLevel;
                $repLvl = $repLevel;

                if (!$valRequired && $value !== null) {
                    $defLvl++;
                }

                if (!isset($rowFirstWrite[$valFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$valFp] = true;
                }

                $valTarget->repetitionLevels[] = $repLvl;
                $valTarget->definitionLevels[] = $defLvl;

                if ($value !== null) {
                    /** @phpstan-ignore assign.propertyType */
                    $valTarget->values[] = $valConverter !== null ? $valConverter->toParquetType($value) : $value;
                }

                $index++;
            }

            return;
        }

        $optKeyFp = $plan->optionalKey['flatPath'];
        $optKeyTarget = $plan->optionalKey['target'];

        if ($valuePlan instanceof ListPlan) {
            if ($mapValue === null) {
                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$optKeyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$optKeyFp] = true;
                }

                $optKeyTarget->repetitionLevels[] = $repLvl;
                $optKeyTarget->definitionLevels[] = $definitionLevel;
                $this->execList($valuePlan, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            if (!$plan->isRequired) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                if ($shouldValidate && $plan->valueColumn !== null) {
                    $this->validator->validate($plan->valueColumn, null);
                }

                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$optKeyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$optKeyFp] = true;
                }

                $optKeyTarget->repetitionLevels[] = $repLvl;
                $optKeyTarget->definitionLevels[] = $definitionLevel;
                $this->execList($valuePlan, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            $definitionLevel++;

            $keyFp = $keyPlan->flatPath;
            $keyTarget = $keyPlan->target;
            $keyRequired = $keyPlan->isRequired;
            $keyConverter = $keyPlan->converter;
            $index = 0;

            foreach ($mapValue as $key => $value) {
                $repLevel = $index === 0 ? $repetitionLevel - 1 : $depth;

                $defLvl = $definitionLevel;
                $repLvl = $repLevel;

                if (!$keyRequired) {
                    $defLvl++;
                }

                if (!isset($rowFirstWrite[$keyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$keyFp] = true;
                }

                $keyTarget->repetitionLevels[] = $repLvl;
                $keyTarget->definitionLevels[] = $defLvl;
                /** @phpstan-ignore assign.propertyType */
                $keyTarget->values[] = $keyConverter !== null ? $keyConverter->toParquetType($key) : $key;
                /** @phpstan-ignore-next-line */
                $this->execList($valuePlan, $value, $definitionLevel, $repLevel, $depth, $shouldValidate, $rowFirstWrite);
                $index++;
            }

            return;
        }

        if ($valuePlan instanceof MapPlan) {
            if ($mapValue === null) {
                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$optKeyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$optKeyFp] = true;
                }

                $optKeyTarget->repetitionLevels[] = $repLvl;
                $optKeyTarget->definitionLevels[] = $definitionLevel;
                $this->execMap($valuePlan, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            if (!$plan->isRequired) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                if ($shouldValidate && $plan->valueColumn !== null) {
                    $this->validator->validate($plan->valueColumn, null);
                }

                $repLvl = $repetitionLevel - 1;

                if (!isset($rowFirstWrite[$optKeyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$optKeyFp] = true;
                }

                $optKeyTarget->repetitionLevels[] = $repLvl;
                $optKeyTarget->definitionLevels[] = $definitionLevel;
                $this->execMap($valuePlan, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

                return;
            }

            $definitionLevel++;

            $keyFp = $keyPlan->flatPath;
            $keyTarget = $keyPlan->target;
            $keyRequired = $keyPlan->isRequired;
            $keyConverter = $keyPlan->converter;
            $index = 0;

            foreach ($mapValue as $key => $value) {
                $repLevel = $index === 0 ? $repetitionLevel - 1 : $depth;

                $defLvl = $definitionLevel;
                $repLvl = $repLevel;

                if (!$keyRequired) {
                    $defLvl++;
                }

                if (!isset($rowFirstWrite[$keyFp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$keyFp] = true;
                }

                $keyTarget->repetitionLevels[] = $repLvl;
                $keyTarget->definitionLevels[] = $defLvl;
                /** @phpstan-ignore assign.propertyType */
                $keyTarget->values[] = $keyConverter !== null ? $keyConverter->toParquetType($key) : $key;
                /** @phpstan-ignore-next-line */
                $this->execMap($valuePlan, $value, $definitionLevel, $repLevel, $depth, $shouldValidate, $rowFirstWrite);
                $index++;
            }

            return;
        }

        /** @var StructPlan $valuePlan */
        if ($mapValue === null) {
            $repLvl = $repetitionLevel - 1;

            if (!isset($rowFirstWrite[$optKeyFp])) {
                $repLvl = 0;
                $rowFirstWrite[$optKeyFp] = true;
            }

            $optKeyTarget->repetitionLevels[] = $repLvl;
            $optKeyTarget->definitionLevels[] = $definitionLevel;
            $this->execStruct($valuePlan, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

            return;
        }

        if (!$plan->isRequired) {
            $definitionLevel++;
        }

        if (!\count($mapValue)) {
            if ($shouldValidate && $plan->valueColumn !== null) {
                $this->validator->validate($plan->valueColumn, null);
            }

            $repLvl = $repetitionLevel - 1;

            if (!isset($rowFirstWrite[$optKeyFp])) {
                $repLvl = 0;
                $rowFirstWrite[$optKeyFp] = true;
            }

            $optKeyTarget->repetitionLevels[] = $repLvl;
            $optKeyTarget->definitionLevels[] = $definitionLevel;
            $this->execStruct($valuePlan, null, $definitionLevel, $repetitionLevel - 1, $depth, $shouldValidate, $rowFirstWrite);

            return;
        }

        $definitionLevel++;

        $keyFp = $keyPlan->flatPath;
        $keyTarget = $keyPlan->target;
        $keyRequired = $keyPlan->isRequired;
        $keyConverter = $keyPlan->converter;
        $index = 0;

        foreach ($mapValue as $key => $value) {
            $repLevel = $index === 0 ? $repetitionLevel - 1 : $depth;

            $defLvl = $definitionLevel;
            $repLvl = $repLevel;

            if (!$keyRequired) {
                $defLvl++;
            }

            if (!isset($rowFirstWrite[$keyFp])) {
                $repLvl = 0;
                $rowFirstWrite[$keyFp] = true;
            }

            $keyTarget->repetitionLevels[] = $repLvl;
            $keyTarget->definitionLevels[] = $defLvl;
            /** @phpstan-ignore assign.propertyType */
            $keyTarget->values[] = $keyConverter !== null ? $keyConverter->toParquetType($key) : $key;
            $this->execStruct($valuePlan, $value, $definitionLevel, $repLevel, $depth, $shouldValidate, $rowFirstWrite);
            $index++;
        }
    }

    /**
     * @param array<string, bool> $rowFirstWrite
     */
    private function execStruct(StructPlan $plan, mixed $structureData, int $definitionLevel, int $repetitionLevel, int $depth, bool $shouldValidate, array &$rowFirstWrite) : void
    {
        if ($structureData === null) {
            foreach ($plan->children as $child) {
                if ($child instanceof FlatPlan) {
                    $fp = $child->flatPath;
                    $target = $child->target;

                    $repLvl = $repetitionLevel;

                    if (!isset($rowFirstWrite[$fp])) {
                        $repLvl = 0;
                        $rowFirstWrite[$fp] = true;
                    }

                    $target->repetitionLevels[] = $repLvl;
                    $target->definitionLevels[] = $definitionLevel;

                    continue;
                }

                if ($child instanceof ListPlan) {
                    $this->execList($child, null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);

                    continue;
                }

                if ($child instanceof MapPlan) {
                    $this->execMap($child, null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);

                    continue;
                }

                /** @var StructPlan $child */
                $this->execStruct($child, null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);
            }

            return;
        }

        if (!$plan->isRequired) {
            $definitionLevel++;
        }

        if (!\is_array($structureData) || !\count($structureData)) {
            foreach ($plan->children as $child) {
                if ($child instanceof FlatPlan) {
                    $fp = $child->flatPath;
                    $target = $child->target;

                    $repLvl = $repetitionLevel;

                    if (!isset($rowFirstWrite[$fp])) {
                        $repLvl = 0;
                        $rowFirstWrite[$fp] = true;
                    }

                    $target->repetitionLevels[] = $repLvl;
                    $target->definitionLevels[] = $definitionLevel;

                    continue;
                }

                if ($child instanceof ListPlan) {
                    $this->execList($child, null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);

                    continue;
                }

                if ($child instanceof MapPlan) {
                    $this->execMap($child, null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);

                    continue;
                }

                /** @var StructPlan $child */
                $this->execStruct($child, null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);
            }

            return;
        }

        foreach ($plan->children as $child) {
            if ($child instanceof FlatPlan) {
                $fp = $child->flatPath;
                $target = $child->target;
                $value = $structureData[$child->childName] ?? null;

                $defLvl = $definitionLevel;
                $repLvl = $repetitionLevel;

                if (!$child->isRequired && $value !== null) {
                    $defLvl++;
                }

                if (!isset($rowFirstWrite[$fp])) {
                    $repLvl = 0;
                    $rowFirstWrite[$fp] = true;
                }

                $target->repetitionLevels[] = $repLvl;
                $target->definitionLevels[] = $defLvl;

                if ($value !== null) {
                    $converter = $child->converter;
                    $target->values[] = $converter !== null ? $converter->toParquetType($value) : $value;
                }

                continue;
            }

            if ($child instanceof ListPlan) {
                $this->execList($child, $structureData[$child->childName] ?? null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);

                continue;
            }

            if ($child instanceof MapPlan) {
                $this->execMap($child, $structureData[$child->childName] ?? null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);

                continue;
            }

            /** @var StructPlan $child */
            $this->execStruct($child, $structureData[$child->childName] ?? null, $definitionLevel, $repetitionLevel, $depth, $shouldValidate, $rowFirstWrite);
        }
    }
}
