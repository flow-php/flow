<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\DSL;

use CuyZ\Valinor\Mapper\TreeMapper;
use CuyZ\Valinor\MapperBuilder;
use Flow\PostgreSql\Bridge\Valinor\{ValinorBuilderMapper, ValinorTreeMapper};

/**
 * @template T of object
 *
 * @param class-string<T> $class
 *
 * @return ValinorTreeMapper<T>
 */
function valinor_tree_mapper(TreeMapper $mapper, string $class) : ValinorTreeMapper
{
    return new ValinorTreeMapper($mapper, $class);
}

/**
 * @template T of object
 *
 * @param class-string<T> $class
 *
 * @return ValinorBuilderMapper<T>
 */
function valinor_builder_mapper(MapperBuilder $builder, string $class) : ValinorBuilderMapper
{
    return new ValinorBuilderMapper($builder, $class);
}
