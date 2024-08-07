public function eval(Row $row) : mixed
{
    // Retrieving parameters
    $left = $this->leftRef->eval($row);
    $right = $this->rightRef->eval($row);

    // Validating parameters
    if ($right === 0) {
        return null;
    }

    if (!\is_numeric($left) || !\is_numeric($right)) {
        return null;
    }

    // Execution
    return $left ** $right;
}