<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

enum Format
{
    /**
     * One line per node with labelled details under it.
     */
    case tree;

    /**
     * A box per node, children side by side.
     */
    case boxes;

    /**
     * The tree turned around: sources first, each node followed by the nodes that read it.
     */
    case flow;

    /**
     * One line per node with the declarations optimizer rules read.
     */
    case declarations;
}
