<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

interface Column
{
    public function ddl() : array;

    public function flatPath() : string;

    public function isList() : bool;

    public function isListElement() : bool;

    public function isMap() : bool;

    public function isMapElement() : bool;

    public function isStruct() : bool;

    public function isStructElement() : bool;

    public function logicalType() : ?LogicalType;

    public function maxDefinitionsLevel() : int;

    public function maxRepetitionsLevel() : int;

    public function name() : string;

    public function normalize() : array;

    public function parent() : ?self;

    public function repetition() : ?Repetition;

    public function setParent(NestedColumn $parent) : void;

    public function type() : ?PhysicalType;

    public function typeLength() : ?int;
}
