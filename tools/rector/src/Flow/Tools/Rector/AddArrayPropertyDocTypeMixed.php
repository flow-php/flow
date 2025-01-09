<?php

declare(strict_types=1);

namespace Flow\Tools\Rector;

use PhpParser\Node;
use PhpParser\Node\Stmt\Property;
use PhpParser\Node\{Identifier, NullableType, UnionType};
use Rector\BetterPhpDocParser\PhpDocInfo\PhpDocInfoFactory;
use Rector\BetterPhpDocParser\PhpDocManipulator\PhpDocTypeChanger;
use Rector\Rector\AbstractRector;
use Symplify\RuleDocGenerator\ValueObject\RuleDefinition;

final class AddArrayPropertyDocTypeMixed extends AbstractRector
{
    use ArrayDocBlockType;

    public function __construct(
        private PhpDocInfoFactory $phpDocInfoFactory,
        private PhpDocTypeChanger $phpDocTypeChanger,
    ) {
    }

    /**
     * @return array<string>
     */
    public function getNodeTypes() : array
    {
        return [Property::class];
    }

    public function getRuleDefinition() : RuleDefinition
    {
        return new RuleDefinition('', []);
    }

    /**
     * @param Property $node
     */
    public function refactor(Node $node) : ?Node
    {
        $this->processProperty($node);

        return null;
    }

    private function isArrayType(Node $type) : bool
    {
        if ($type instanceof Identifier) {
            return $type->toString() === 'array';
        }

        if ($type instanceof UnionType) {
            foreach ($type->types as $subType) {
                if ($subType instanceof Identifier && $subType->toString() === 'array') {
                    return true;
                }
            }
        }

        if ($type instanceof NullableType) {
            return $this->isArrayType($type->type);
        }

        return false;
    }

    private function processProperty(Property $property) : void
    {
        if (!$property->type) {
            return;
        }

        $type = $property->type;

        if (!$this->isArrayType($type)) {
            return;
        }

        $phpDocInfo = $this->phpDocInfoFactory->createFromNodeOrEmpty($property);

        if ($phpDocInfo->hasByName('var')) {
            return;
        }

        $newType = $this->resolveType($type);
        $this->phpDocTypeChanger->changeVarType($property, $phpDocInfo, $newType);
    }
}
