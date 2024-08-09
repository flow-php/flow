public function eval(Row $row) : float|int|null
{
    // Retrieving parameters
    $left = (new Parameter($this->left))->asNumber($row);
    $right = (new Parameter($this->right))->asNumber($row);

    // Validating parameters
    if ($left === null || $right === null) {
        return null;
    }

    if ($right === 0) {
        return null;
    }

    // Execution
    return $left ** $right;
}